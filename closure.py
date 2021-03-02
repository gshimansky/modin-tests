def outer_func():
    msg="String1"
    def func1():
        nonlocal msg
        print("func1", msg)
        msg="String2"
        print("func1", msg)

    def func2():
        nonlocal msg
        print("func2", msg)
        msg="String3"
        print("func2", msg)

    return (func1, func2)

myfuncs = outer_func()

myfuncs[0]()
myfuncs[1]()
