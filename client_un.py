import Pyro5.api

def main():
    with Pyro5.api.Proxy("PYRONAME:unnumbered.server") as s:

if __name__ == "__main__":
    main()
