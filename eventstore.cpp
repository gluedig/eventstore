#include <string>
#include <set>
#include <atomic>
#include <thread>
#include <chrono>
#include <mutex>

#include <string.h>
#include <signal.h>
#include <assert.h>
#include <czmq.h>
#include <pthread.h>
#include <stdio.h>
#include <sqlite3.h>

#define BEACON_PORT 5555
#define CLEANUP_PERIOD 60
#define EVENTS_LIMIT 10000


static int count_events(sqlite3 *sql, int *count);

struct CleanupFunction {
        void cleanup(sqlite3 *database, std::atomic_bool *run, int *lost_events, int limit) {
                while (run->load()) {
                        int count;
                        std::this_thread::sleep_for(std::chrono::seconds(CLEANUP_PERIOD));
                        if (! run->load())
                                break;

                        count_events(database, &count);
                        if (count > limit) {
                                int to_remove = count - limit;
                                fprintf(stderr, "cleanup removing %d events (limit %d total %d)\n", to_remove, limit, count);
                                char *zSQL = sqlite3_mprintf("DELETE FROM events WHERE id IN (SELECT id FROM EVENTS ORDER BY id LIMIT %d)", to_remove);
                                char *err_msg = 0;
                                if (sqlite3_exec(database, zSQL, 0, 0, &err_msg)) {
                                        fprintf(stderr, "Cannot remove events form db %s\n", err_msg);
                                        sqlite3_free(err_msg);
                                } else {
                                        *lost_events += to_remove;
                                }
                                sqlite3_free(zSQL);
                        }
                }
        }
};

zctx_t *ctx;
zbeacon_t *beacon_ctx;
zmq_pollitem_t *pollitems;
std::set<std::string> poll_addrs;
sqlite3 *db;

CleanupFunction *cf;
std::thread *cleanup_thread;
std::atomic_bool *cleanup_thread_flag;
std::once_flag terminate_flag;

int lost_events = 0;
int processed_events = 0;
int store_id = -1;

void finish(int ret)
{
        fprintf(stderr, "Terminating, waiting for threads for finish...\n");
        if (beacon_ctx) {
                zbeacon_unsubscribe(beacon_ctx);
                zbeacon_destroy(&beacon_ctx);
        }

        free(pollitems);

        if (ctx)
                zctx_destroy(&ctx);

        if(cleanup_thread) {
                cleanup_thread_flag->store(false);
                cleanup_thread->join();
                delete cf;
                delete cleanup_thread;
                delete cleanup_thread_flag;
        }

        int count = 0;
        if(db) {
                count_events(db, &count);
                sqlite3_close(db);
        }

        fprintf(stderr, "Done, total processed events %d lost events %d events in db %d\n",
                processed_events, lost_events, count);

        exit(ret);
}

void terminate_handler(int sig)
{
        signal(sig, SIG_DFL);
        std::call_once(terminate_flag, finish, 0);
}

zmq_pollitem_t *new_pollentry(std::string addr, zmq_pollitem_t *items, int *items_no)
{
        zmq_pollitem_t *new_items = (zmq_pollitem_t *)malloc(((*items_no)+1) * sizeof(zmq_pollitem_t));
        memset(new_items, 0, *items_no);
        memcpy(new_items, items, (*items_no) * sizeof(zmq_pollitem_t));

        zmq_pollitem_t *new_item = &new_items[*items_no];
        new_item->socket = zsocket_new(ctx, ZMQ_SUB);
        assert(new_item->socket);
        new_item->fd = 0;
        new_item->events = ZMQ_POLLIN | ZMQ_POLLERR;
        new_item->revents = 0;
        zsocket_connect(new_item->socket, addr.c_str());
        int rc = zmq_setsockopt(new_item->socket, ZMQ_SUBSCRIBE, "", 0);
        assert(!rc);

        (*items_no)++;
        free(items);
        return new_items;
}

static int event_count_clbk(void *count, int argc, char **argv, char **column_names)
{
        int *events_no = (int *)count;
        int i;
        for(i=0; i<argc; i++) {
//                printf("%s = %s\n", column_names[i], argv[i] ? argv[i] : "NULL");
                if (!strcmp(column_names[i], "COUNT(*)") && argv[i]) {
                        *events_no = atoi(argv[i]);
                        break;
                }
        }

        return SQLITE_OK;
}

static const char *count_events_sql = "SELECT COUNT(*) FROM events;";
static int count_events(sqlite3 *sql, int *count)
{
        char *err_msg = 0;
        int err = SQLITE_OK;
        if ((err = sqlite3_exec(sql, count_events_sql, event_count_clbk, count, &err_msg))) {
                fprintf(stderr, "Cannot get events count: %s\n", err_msg);
                sqlite3_free(err_msg);
        }

        return err;
}

static int event_no_clbk(void *count, int argc, char **argv, char **column_names)
{
        int *events_no = (int *)count;
        int i;
        for(i=0; i<argc; i++) {
//                printf("%s = %s\n", column_names[i], argv[i] ? argv[i] : "NULL");
                if (!strcmp(column_names[i], "ID") && argv[i]) {
                        *events_no = atoi(argv[i]);
                        break;
                }
        }

        return SQLITE_OK;
}

static int get_event_no(sqlite3 *sql, int *count, bool last)
{
        char *err_msg = 0;
        int err = SQLITE_OK;
        static const char *first_eventno_sql = "SELECT id FROM events ORDER BY id ASC LIMIT (1)";
        static const char *last_eventno_sql = "SELECT id FROM events ORDER BY id DESC LIMIT (1)";
        if ((err = sqlite3_exec(sql, last ? last_eventno_sql : first_eventno_sql,
                                event_no_clbk, count, &err_msg))) {
                fprintf(stderr, "Cannot get event number: %s\n", err_msg);
                sqlite3_free(err_msg);
        }

        return err;
}

static const char *db_file = "eventstore.db";
static const char *create_tables_sql = "CREATE TABLE events(ID INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT);";
static sqlite3 *open_or_create_db()
{
        sqlite3 *tmp;
        int err;
        if ((err = sqlite3_open_v2(db_file, &tmp, SQLITE_OPEN_READWRITE, NULL)) == SQLITE_OK) {
                int event_count = -1;
                fprintf(stderr, "found existing db, opening\n");
                if (count_events(tmp, &event_count)) {
                        sqlite3_close(tmp);
                        finish(1);
                }
                fprintf(stderr, "%d event(s) already in db\n", event_count);
                return tmp;
        } else {
                if (err == SQLITE_CANTOPEN) {
                        sqlite3_close(tmp);
                        if (sqlite3_open_v2(db_file, &tmp, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL)) {
                                fprintf(stderr, "cant open new db: %s\n", sqlite3_errmsg(tmp));
                                sqlite3_close(tmp);
                                finish(1);
                        }
                        fprintf(stderr, "created new db file\n");
                        char *err_msg = 0;
                        if (sqlite3_exec(tmp, create_tables_sql, NULL, NULL, &err_msg)) {
                                fprintf(stderr, "cannot create table: %s\n", err_msg);
                                sqlite3_free(err_msg);
                                sqlite3_close(tmp);
                                finish(1);
                        }
                        return tmp;
                } else {
                        fprintf(stderr, "cant open db: %s\n", sqlite3_errmsg(tmp));
                        sqlite3_close(tmp);
                        finish(1);
                }
        }
}

static const char progress[] = { '|', '/', '-', '\\'};

static int add_event(sqlite3 *sql, byte *event_data, size_t data_len)
{
        int ret = 0;
        char *data_str = (char *)malloc(data_len+1);
        assert(data_str);
        memset(data_str, 0, data_len+1);
        memcpy(data_str, event_data, data_len);
//        fprintf(stderr, "event len: %ld data: %s\n", data_len, data_str);

        char *zSQL = sqlite3_mprintf("INSERT INTO events (event) VALUES('%q')", data_str);
        char *err_msg = 0;
        if (sqlite3_exec(sql, zSQL, 0, 0, &err_msg)) {
                fprintf(stderr, "Cannot add event to db: %s\n", err_msg);
                sqlite3_free(err_msg);
                ret = 1;
        }
        sqlite3_free(zSQL);

        free(data_str);
        processed_events++;
        fprintf(stdout, "\b%c", progress[processed_events % 4]);
        return ret;
}

static const char *create_meta_table_sql = "CREATE TABLE meta(timestamp INTEGER, store_id INTEGER, first_event INTEGER, last_event INTEGER, lost_events INTEGER);";
int backupDb(sqlite3 *pDb,               /* Database to back up */
             const char *zFilename,      /* Name of file to back up to */
             const time_t *time
            )
{
        int rc;                     /* Function return code */
        sqlite3 *pFile;             /* Database connection opened on zFilename */
        sqlite3_backup *pBackup;    /* Backup handle used to copy data */
        char *err_msg = 0;

        if ((rc = sqlite3_exec(pDb, "VACUUM", NULL, NULL, &err_msg))) {
                fprintf(stderr, "error VACUUMing maind db %s\n", err_msg);
                sqlite3_free(err_msg);
                goto done;

        }

        /* Open the database file identified by zFilename. */
        rc = sqlite3_open(zFilename, &pFile);
        if ( rc==SQLITE_OK ) {

                /* Open the sqlite3_backup object used to accomplish the transfer */
                pBackup = sqlite3_backup_init(pFile, "main", pDb, "main");
                if( pBackup ) {

                        /* Each iteration of this loop copies 5 database pages from database
                        * pDb to the backup database. If the return value of backup_step()
                        * indicates that there are still further pages to copy, sleep for
                        * 250 ms before repeating. */
                        do {
                                rc = sqlite3_backup_step(pBackup, 50);
                                if( rc==SQLITE_OK || rc==SQLITE_BUSY || rc==SQLITE_LOCKED ) {
                                        sqlite3_sleep(250);
                                }
                        } while( rc==SQLITE_OK || rc==SQLITE_BUSY || rc==SQLITE_LOCKED );

                        /* Release resources allocated by backup_init(). */
                        (void)sqlite3_backup_finish(pBackup);
                }
                rc = sqlite3_errcode(pFile);
        }

        if ( rc==SQLITE_OK ) {
                fprintf(stderr, "backup finished, adding metadata\n");
                if ((rc = sqlite3_exec(pFile, create_meta_table_sql, NULL, NULL, &err_msg))) {
                        fprintf(stderr, "cannot create meta table: %s\n", err_msg);
                        sqlite3_free(err_msg);
                        goto done;
                }
                int first_event;
                if ((rc = get_event_no(pFile, &first_event, false))) {
                        fprintf(stderr, "cannot get firt event no\n");
                        goto done;
                }
                int last_event;
                if ((rc = get_event_no(pFile, &last_event, true))) {
                        fprintf(stderr, "cannot get firt event no\n");
                        goto done;
                }
                char *zSQL = sqlite3_mprintf("INSERT INTO meta (timestamp, store_id, first_event, last_event, lost_events) VALUES(%d, %d, %d, %d, %d)",
                                             *time, store_id, first_event, last_event, lost_events);

                if ((rc = sqlite3_exec(pFile, zSQL, 0, 0, &err_msg))) {
                        fprintf(stderr, "Cannot add meta to backup db: %s\n", err_msg);
                        sqlite3_free(err_msg);
                        sqlite3_free(zSQL);
                        goto done;
                }
                sqlite3_free(zSQL);
        }

done:
        (void)sqlite3_close(pFile);
        return rc;
}

void backup_handler(int sig)
{
        if (sig != SIGUSR1)
                return;

        char date[256];
        char backupfile[256];
        time_t curr_time = time(NULL);

        strftime(date, 256, "%F-%T", localtime(&curr_time));
        sprintf(backupfile, "./backup/%s_%s.bak", db_file, date);
        fprintf(stderr, "starting db backup to %s\n", backupfile);
        int rc;
        if ((rc=backupDb(db, backupfile, &curr_time)) != SQLITE_OK) {
                fprintf(stderr, "error creating backup (%d)\n", rc);
        } else {
                fprintf(stderr, "db backup completed\n");
                lost_events = 0;
        }
}

static const char *event_filter = "EVENT_SRC";

int main(int argc, char **argv)
{
        int limit = EVENTS_LIMIT;

        if (argc < 2) {
                fprintf(stderr, "usage: %s store_id [event limit]\n",argv[0]);
                exit(1);
        } else {
                store_id = atoi(argv[1]);
                if (argc == 3)
                        limit = atoi(argv[2]);
        }
        fprintf(stderr, "Starting, event store id: 0x%x events limit %d\n", store_id, limit);


        sigset_t signal_mask;
        sigemptyset(&signal_mask);
        sigaddset(&signal_mask, SIGINT);
        sigaddset(&signal_mask, SIGUSR1);
        int rc = pthread_sigmask (SIG_BLOCK, &signal_mask, NULL);
        if (rc != 0)
                fprintf(stderr, "failed to set sigmask\n");

        ctx = zctx_new ();
        assert (ctx);

        db = open_or_create_db();

        beacon_ctx = zbeacon_new(BEACON_PORT);
        zbeacon_subscribe(beacon_ctx, (byte *)event_filter, strlen(event_filter));

        int no_pollitems = 1;
        pollitems = (zmq_pollitem_t *)malloc(sizeof(zmq_pollitem_t));
        zmq_pollitem_t *beacon_item = &pollitems[0];
        beacon_item->socket = zbeacon_pipe(beacon_ctx);
        beacon_item->fd = 0;
        beacon_item->events = ZMQ_POLLIN;
        beacon_item->revents = 0;

        cf = new CleanupFunction();
        cleanup_thread_flag = new std::atomic_bool(true);
        cleanup_thread = new std::thread(std::bind(&CleanupFunction::cleanup, std::ref(cf), db, cleanup_thread_flag, &lost_events, limit));

        rc = pthread_sigmask (SIG_UNBLOCK, &signal_mask, NULL);
        if (rc != 0)
                fprintf(stderr, "failed to set sigmask\n");

        signal(SIGINT, terminate_handler);
        signal(SIGUSR1, backup_handler);

        while (zmq_poll (pollitems, no_pollitems, -1)) {

                if (pollitems[0].revents & ZMQ_POLLIN) {
                        char *ipaddress = zstr_recv (zbeacon_pipe (beacon_ctx));
                        char *beacon = beacon = zstr_recv (zbeacon_pipe (beacon_ctx));
                        std::string beacon_str = std::string(beacon);
                        size_t first = beacon_str.find_first_of(":");

                        if (first != std::string::npos) {
                                std::string event_src_addr = beacon_str.substr(first+1, beacon_str.size());
                                if (!poll_addrs.count(event_src_addr)) {
                                        fprintf(stderr, "New beacon from: %s\n", event_src_addr.c_str());
                                        pollitems = new_pollentry(event_src_addr, pollitems, &no_pollitems);
                                        poll_addrs.insert(event_src_addr);
                                }
                        }
                        free(ipaddress);
                        free(beacon);
                } else if (no_pollitems > 1) {
                        for (int i=1; i<no_pollitems; i++) {
                                char sock_endpoint[256];
                                size_t endpoint_size = sizeof(sock_endpoint);
                                zmq_getsockopt(pollitems[i].socket, ZMQ_LAST_ENDPOINT, sock_endpoint, &endpoint_size);
//                                fprintf(stderr, "new event from %s\n", sock_endpoint);

                                if (pollitems[i].revents & ZMQ_POLLIN) {
                                        zframe_t *frame = zframe_recv (pollitems[i].socket);
                                        //zframe_print(frame, 0);
                                        add_event(db, zframe_data(frame), zframe_size(frame));
                                        zframe_destroy(&frame);
                                } else if (pollitems[i].revents & ZMQ_POLLERR) {
                                        fprintf(stderr, "ZMO_POLLERR on %s\n", sock_endpoint);
                                        poll_addrs.erase(std::string(sock_endpoint));
                                        zsocket_disconnect(pollitems[i].socket, sock_endpoint);
                                        zsocket_destroy(ctx, pollitems[i].socket);

                                }
                        }
                }
        }


        finish(0);
}
