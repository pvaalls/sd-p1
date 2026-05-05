import Pyro5.api
import itertools
import argparse

@Pyro5.api.expose
class LoadBalancer:

    def __init__(self):
        self.workers = []                   # Worker URIs
        self.counter = itertools.count()    # Contador para RR

    def register_worker(self, uri):
        if uri not in self.workers:
            self.workers.append(uri)
            print(f"[\033[32m+\033[0m] - Worker + :", f"\033[32m{uri}\033[0m")

    def unregister_worker(self, uri):
        if uri in self.workers:
            self.workers.remove(uri)
            print(f"[\033[31m-\033[0m] - Worker - :", f"\033[31m{uri}\033[0m")

    def get_worker(self):
        if not self.workers:
            return None
        return self.workers[next(self.counter) % len(self.workers)]

def main():

    parser = argparse.ArgumentParser(description="Load Balancer (Unnumbered Tickets)")
    parser.add_argument("-n", "--ns",   type=str, default="localhost", help="Nameserver Host (default: %(default)s)")
    parser.add_argument("-H", "--host", type=str, default="localhost", help="Host (default: %(default)s)")
    parser.add_argument("-p", "--port", type=int, default=9000, help="Port (default: %(default)s)")
    args = parser.parse_args()

    lb_name = "ticket.server.numbered"

    daemon  = Pyro5.api.Daemon(host=args.host,port=args.port)
    ns      = Pyro5.api.locate_ns(host=args.ns)

    lb_uri  = daemon.register(LoadBalancer(), objectId="loadbalancer")
    ns.register(lb_name, lb_uri)

    print("[\033[32m+\033[0m] - LoadBalancer running...")
    print("[\033[32m+\033[0m] - NS Entry :", f"\033[32m{lb_name}\033[0m")
    print("[\033[32m+\033[0m] -      URI :", f"\033[32m{lb_uri}\033[0m")

    daemon.requestLoop()

if __name__ == "__main__":
    main()
