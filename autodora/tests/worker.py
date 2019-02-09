import sys

if __name__ == "__main__":
    print(sys.argv[1] if len(sys.argv) > 1 else sys.argv[0], end="")
