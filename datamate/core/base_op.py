class Mapper:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

class MockOperators:
    def register_module(self, module_name, module_path):
        print(f"Registered {module_name} from {module_path}")

OPERATORS = MockOperators()