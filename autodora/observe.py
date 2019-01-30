class Observer:
    def __init__(self):
        self.observers = []

    def add_observer(self, observer):
        self.observers.append(observer)

    def remove_observer(self, observer):
        self.observers.remove(observer)


def dispatch(func):
    def modified(self, *args, **kwargs):
        for observer in self.observers:
            getattr(observer, func.__name__)(*args, **kwargs)
    return modified


class ProgressObserver(Observer):
    @dispatch
    def experiment_started(self, index, experiment):
        raise NotImplementedError()

    @dispatch
    def experiment_finished(self, index, experiment):
        raise NotImplementedError()

    @dispatch
    def experiment_interrupted(self, index, experiment):
        raise NotImplementedError()



