#include <string>
#include <iostream>
#include <set>
#include <signal.h>
#include <assert.h>
#include <czmq.h>
#include <pthread.h>

#define BEACON_PORT 5555
zctx_t *ctx;
zbeacon_t *beacon_ctx;
zmq_pollitem_t *pollitems;
std::set<std::string> poll_addrs;
std::string event_filter = "EVENT_SRC";


void finish(int ret)
{
        if (beacon_ctx) {
                zbeacon_unsubscribe(beacon_ctx);
                zbeacon_destroy(&beacon_ctx);
        }

        free(pollitems);

        if (ctx)
                zctx_destroy(&ctx);

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

int main (int argc, char **argv)
{
        ctx = zctx_new ();
        assert (ctx);

        signal(SIGINT, terminate_handler);

        beacon_ctx = zbeacon_new(BEACON_PORT);
        zbeacon_subscribe(beacon_ctx, (byte *)event_filter.c_str(), event_filter.size());

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
                                        std::cerr << "Got beacon from: " << ipaddress << " <" << beacon_str << ">\n";
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
                                std::cerr << "new event from " << sock_endpoint << std::endl;

                                if (pollitems[i].revents & ZMQ_POLLIN) {
                                        zframe_t *frame = zframe_recv (pollitems[i].socket);
                                        //zframe_print(frame, 0);
                                        std::cout << zframe_data(frame) << std::endl;
                                        zframe_destroy(&frame);
                                } else if (pollitems[i].revents & ZMQ_POLLERR) {
                                        std::cerr << "ZMO_POLLERR on " << sock_endpoint << std::endl;
                                        poll_addrs.erase(std::string(sock_endpoint));
                                        zsocket_disconnect(pollitems[i].socket, sock_endpoint);
                                        zsocket_destroy(ctx, pollitems[i].socket);

                                }
                        }
                }
        }


        finish(0);
}
