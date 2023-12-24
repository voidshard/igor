
try:
    from igorUI import ui
except ImportError:
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    print(sys.path)
    from igorUI import ui


def main():
    ui.start()


if __name__ == "__main__":
    main()
