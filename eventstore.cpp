#include <string>
#include <iostream>
#include <assert.h>
#include <czmq.h>

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

int main (int argc, char **argv)
{
        ctx = zctx_new ();
        assert (ctx);
        std::string event_filter = "EVENT_SRC";

        beacon_ctx = zbeacon_new(BEACON_PORT);
        zbeacon_subscribe(beacon_ctx, (byte *)event_filter.c_str(), event_filter.size());
        zmq_pollitem_t pollitems [] = {
                { zbeacon_pipe (beacon_ctx), 0, ZMQ_POLLIN, 0 },
        };


        if (!zmq_poll (pollitems, 1, -1))
                finish(1); //Interrupted

        char *beacon;
        if (pollitems [0].revents & ZMQ_POLLIN) {
                char *ipaddress = zstr_recv (zbeacon_pipe (beacon_ctx));
                beacon = zstr_recv (zbeacon_pipe (beacon_ctx));
                std::cerr << "Got beacon from: " << ipaddress << " <" << beacon << ">\n";
                free (ipaddress);
        } else {
                std::cerr << "Unknown event on beacon pipe: " << pollitems[0].revents << std::endl;
                finish(1);
        }

        std::string beacon_str = std::string(beacon);
        free(beacon);
        size_t first = beacon_str.find_first_of(":");
        if (first != std::string::npos) {
                std::string event_src_addr = beacon_str.substr(first+1, beacon_str.size());
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
        finish(0);
}
