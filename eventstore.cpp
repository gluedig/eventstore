#include <string>
#include <set>

#include <string.h>
#include <signal.h>
#include <assert.h>
#include <czmq.h>
#include <pthread.h>
#include <stdio.h>
#include <sqlite3.h>

#define BEACON_PORT 5555
static const char *event_filter = "EVENT_SRC";
static const char *db_file = "eventstore.db";
static const char *create_tables_sql = "CREATE TABLE events(ID INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT);";
static const char *count_events_sql = "SELECT COUNT(*) FROM events;";

zctx_t *ctx;
zbeacon_t *beacon_ctx;
zmq_pollitem_t *pollitems;
std::set<std::string> poll_addrs;
sqlite3 *db;

void finish(int ret)
{
        if (beacon_ctx) {
                zbeacon_unsubscribe(beacon_ctx);
                zbeacon_destroy(&beacon_ctx);
        }

        free(pollitems);

        if (ctx)
                zctx_destroy(&ctx);

        if(db)
                sqlite3_close(db);
        exit(ret);
}

void terminate_handler(int sig)
{
        signal(sig, SIG_DFL);
        finish(0);
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

static int add_event(sqlite3 *sql, byte *event_data, size_t data_len)
{
        int ret = 0;
        char *data_str = (char *)malloc(data_len+1);
        assert(data_str);
        memset(data_str, 0, data_len+1);
        memcpy(data_str, event_data, data_len);
        fprintf(stderr, "event len: %ld data: %s\n", data_len, data_str);

        char *zSQL = sqlite3_mprintf("INSERT INTO events (event) VALUES('%q')", data_str);
        char *err_msg = 0;
        if (sqlite3_exec(sql, zSQL, 0, 0, &err_msg)) {
                fprintf(stderr, "Cannot add event to db: %s\n", err_msg);
                sqlite3_free(err_msg);
                ret = 1;
        }
        sqlite3_free(zSQL);

        free(data_str);
        return ret;
}

int backupDb(sqlite3 *pDb,               /* Database to back up */
             const char *zFilename      /* Name of file to back up to */
            )
{
        int rc;                     /* Function return code */
        sqlite3 *pFile;             /* Database connection opened on zFilename */
        sqlite3_backup *pBackup;    /* Backup handle used to copy data */

        /* Open the database file identified by zFilename. */
        rc = sqlite3_open(zFilename, &pFile);
        if( rc==SQLITE_OK ) {

                /* Open the sqlite3_backup object used to accomplish the transfer */
                pBackup = sqlite3_backup_init(pFile, "main", pDb, "main");
                if( pBackup ) {

                        /* Each iteration of this loop copies 5 database pages from database
                        * pDb to the backup database. If the return value of backup_step()
                        * indicates that there are still further pages to copy, sleep for
                        * 250 ms before repeating. */
                        do {
                                rc = sqlite3_backup_step(pBackup, 5);
                                if( rc==SQLITE_OK || rc==SQLITE_BUSY || rc==SQLITE_LOCKED ) {
                                        sqlite3_sleep(250);
                                }
                        } while( rc==SQLITE_OK || rc==SQLITE_BUSY || rc==SQLITE_LOCKED );

                        /* Release resources allocated by backup_init(). */
                        (void)sqlite3_backup_finish(pBackup);
                }
                rc = sqlite3_errcode(pFile);
        }

        /* Close the database connection opened on database file zFilename
        *   ** and return the result of this function. */
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
        fprintf(stderr, "startind db backup to %s\n", backupfile);
        int rc;
        if ((rc=backupDb(db, backupfile)) != SQLITE_OK) {
                fprintf(stderr, "error creating backup (%d)\n", rc);
        } else {
                fprintf(stderr, "db backup completed\n");
        }
}


int main(int argc, char **argv)
{
        ctx = zctx_new ();
        assert (ctx);

        signal(SIGINT, terminate_handler);
        signal(SIGUSR1, backup_handler);
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
                                fprintf(stderr, "new event from %s\n", sock_endpoint);

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
