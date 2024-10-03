import datetime


def log(*args, end="\n"):
    dt = datetime.datetime.now().strftime("[%H:%M:%S] ->")
    print("{}".format(dt), " ".join([str(i) for i in args]), end=end)

