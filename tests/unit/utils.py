import os
import sys
import betamax

class Botomax:
    def __init__(self, pytest_request=None):
        self.pytest_request = pytest_request
        self.recorder = None

        path = os.path.dirname(os.path.realpath(pytest_request.fspath.__str__()))
        self.cassette_shelf = os.path.join(path, 'botomax_cassettes')

    def press_record_button(self, session):
        self.mask_credentials()
        self.recorder = betamax.Betamax(session)
        self.recorder.use_cassette(self.cassette_path())
        self.recorder.start()
        self.pytest_request.addfinalizer(self.stop_recorder)

    def mask_credentials(self):
        with betamax.Betamax.configure() as config:
            for placeholder in self.get_placeholders():
                config.define_cassette_placeholder(placeholder['masked'], placeholder['unmasked'])

    def get_placeholders(self):
        placeholders = []
        try:
            __import__('secrets')
            module = sys.modules['secrets']
            for v in dir(module): 
                if not v.startswith('__') and isinstance(getattr(module, v), str):
                    placeholders.append({'masked':'<'+v.upper()+'>', 'unmasked':getattr(module,v)})
        except ImportError as e:
            print(e)
        return placeholders

    def stop_recorder(self):
        tmp = []
        for interaction in self.recorder.current_cassette.interactions:
            interaction.json = self.decode_interaction(interaction.json)
            tmp.append(interaction)
        self.recorder.current_cassette.interactions = tmp
        self.recorder.stop()

    def cassette_name(self):
        cassette_name = ''
        if self.pytest_request.module is not None:
            cassette_name += self.pytest_request.module.__name__ + '.'

        if self.pytest_request.cls is not None:
            cassette_name += self.pytest_request.cls.__name__ + '.'

        if self.pytest_request.function is not None:
            cassette_name += self.pytest_request.function.__name__
        return cassette_name

    def cassette_path(self):
        if not os.path.exists(self.cassette_shelf):
            os.makedirs(self.cassette_shelf)
        return os.path.join(self.cassette_shelf,  self.cassette_name())

    def decode_interaction(self, interaction):
        if isinstance(interaction, dict):
            new_dict = {}
            for (k,v) in interaction.items():
                new_dict[k] = self.decode_interaction(v)
            return new_dict
        elif isinstance(interaction, list):
            new_list = []
            for entry in interaction:
                new_list.append(self.decode_interaction(entry))
            return new_list
        elif type(interaction) == bytes:
            return interaction.decode()
        elif type(interaction) == str:
            return interaction
        else:
            return interaction

