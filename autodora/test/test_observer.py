from autodora.observe import Observer, dispatch


class MyObserver(Observer):
    @dispatch
    def method(self, arg):
        raise NotImplementedError()


class MyObserver1(MyObserver):
    def __init__(self, expected: list):
        super().__init__()
        self.expected = expected

    def method(self, arg):
        expected = self.expected.pop(0)
        assert expected == arg

    def done(self):
        assert len(self.expected) == 0


def test_observer_simple():
    message1 = "hello"
    observer1 = MyObserver1([message1])
    observer1.method(message1)
    observer1.done()


def test_observer_dispatch():
    message1 = "hello"
    message2 = "hello again"
    message3 = "hello once more"
    message4 = "good bye"
    message5 = "rise from the dead"

    dispatcher1 = MyObserver()
    print(dispatcher1.method)
    dispatcher1.method(message1)

    observer1 = MyObserver1([message1, message3, message4, message4, message5])
    dispatcher1.add_observer(observer1)

    dispatcher2 = MyObserver()
    dispatcher1.add_observer(dispatcher2)

    observer2 = MyObserver1([message2, message1, message3, message4, message5])
    dispatcher2.add_observer(observer2)

    dispatcher2.method(message2)
    dispatcher1.method(message1)

    dispatcher2.add_observer(observer1)

    dispatcher2.method(message3)
    dispatcher1.method(message4)

    dispatcher2.remove_observer(observer1)
    dispatcher1.method(message5)

    observer1.done()
    observer2.done()
