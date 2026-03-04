import Pyro5.api

@Pyro5.api.expose
class Server:
    def __init__ ( self ):

def main():
    daemon = Pyro5.api.Daemon()             # Create daemon
    ns     = Pyro5.api.locate_ns()          # Locate name server
    uri    = daemon.register(Server)        # Register class
    ns.register("unnumbered.server", uri)   # Register with name server

    print("Server is running...")
    daemon.requestLoop()                    # Start loop

if __name__ == "__main__":
    main()