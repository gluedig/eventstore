#include <string>
#include <iostream>
#include <list>
#include <signal.h>
#include <assert.h>
#include <czmq.h>
#include <pthread.h>

#define BEACON_PORT 5555
zctx_t *ctx;
zbeacon_t *beacon_ctx;

void finish(int ret)
{

        if (beacon_ctx) {
                zbeacon_unsubscribe(beacon_ctx);
                zbeacon_destroy(&beacon_ctx);
        }

        if (ctx)
                zctx_destroy(&ctx);
        exit(ret);
}


void terminate_handler(int sig)
{
        signal(sig, SIG_DFL);
        finish(0);
}

void new_pollentry(std::string addr, zmq_pollitem_t *items, int *items_no)
{
/*
               std::cerr << "Connecting to: " << event_src_addr << std::endl;
                void *input = zsocket_new (ctx, ZMQ_SUB);
                assert (input);
                zsocket_connect (input, event_src_addr.c_str());

                int rc = zmq_setsockopt (input, ZMQ_SUBSCRIBE, "", 0);
                assert (rc == 0);

                int i;
                for (i = 0; i < 10; i++) {
                        zframe_t *frame = zframe_recv (input);
                        zframe_print(frame, "got: ");
                        zframe_destroy(&frame);
                }
                zsocket_disconnect(input, event_src_addr.c_str());
                zsocket_destroy(ctx, input);
        }
  */
        int new_item_no = *items_no;
        zmq_pollitem_t *new_items = (zmq_pollitem_t*)malloc(((*items_no)+1) * sizeof(zmq_pollitem_t));
        memcpy(new_items, items, *items_no); 


        zmq_pollitem_t *new_item = &new_items[new_item_no];
        new_item->socket = zsocket_new(ctx, ZMQ_SUB);
        new_item->fd = 0;
        new_item->events = ZMQ_POLLIN | ZMQ_POLLERR;
        new_item->revents = 0;
        assert(new_item->socket);

        (*items_no)++;
}

int main (int argc, char **argv)
{
        ctx = zctx_new ();
        assert (ctx);

        signal(SIGINT, terminate_handler);
        std::string event_filter = "EVENT_SRC";
        
        beacon_ctx = zbeacon_new(BEACON_PORT);
        zbeacon_subscribe(beacon_ctx, (byte *)event_filter.c_str(), event_filter.size());

        int no_pollitems = 1;
        zmq_pollitem_t pollitems[] = {
                { zbeacon_pipe(beacon_ctx), 0, ZMQ_POLLIN, 0},
        };

        while (zmq_poll (pollitems, no_pollitems, -1)) {

                if (pollitems[0].revents & ZMQ_POLLIN) {
                        char *ipaddress = zstr_recv (zbeacon_pipe (beacon_ctx));
                        char *beacon = beacon = zstr_recv (zbeacon_pipe (beacon_ctx));
                        std::string beacon_str = std::string(beacon);
                        std::cerr << "Got beacon from: " << ipaddress << " <" << beacon_str << ">\n";
                        free(ipaddress);
                        free(beacon);
                        size_t first = beacon_str.find_first_of(":");
                        if (first != std::string::npos) {
                                std::string event_src_addr = beacon_str.substr(first+1, beacon_str.size());
                                
                        }
                } else if (no_pollitems > 1) {


                }
        }
       

      finish(0);
}
