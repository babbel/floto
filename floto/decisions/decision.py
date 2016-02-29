class Decision:
    def __init__(self):
        self.required_fields = []

    def get_decision(self):
        d = self._get_decision()
        self.assert_required_fields(d, self.required_fields)
        return d

    def _get_decision(self):
        raise NotImplementedError

    def assert_required_fields(self, decision, fields):
        for path in fields:
            if not self.path_in_dictionary(decision, path):
                message = "Decission {0} must have key: {1}".format(self.__class__.__name__, path)
                raise KeyError(message)

    def path_in_dictionary(self, dictionary, path):
        """Checks if a field at given path exists in the dictionary
        Parameters
        ----------
        dictionary: dict
        path: str
            Path to the key: key1.key2.key<n>

        Returns
        -------
        bool
            True if path exists and the value is not None, False otherwise
        """
        if path:
            key = path.split('.')[0]
            if key in dictionary and dictionary[key]:
                key_exists = self.path_in_dictionary(dictionary[key], '.'.join(path.split('.')[1:]))
            else:
                key_exists = False
        else:
            key_exists = True
        return key_exists
