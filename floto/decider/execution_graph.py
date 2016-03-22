import logging

logger = logging.getLogger(__name__)

class ExecutionGraph:
    def __init__(self, activity_tasks=None):
        self.tasks = activity_tasks

        self._id_to_idx = None
        self._idx_to_id = None
        self._ids = None
        self._tasks_by_id = None

        self.graph_matrix = None

    @property
    def graph(self):
        if not self.graph_matrix:
            self.graph_matrix = self.graph_from_task_specs()
        return self.graph_matrix

    @property
    def id_to_idx(self):
        if not self._id_to_idx:
            self.generate_indices()
        return self._id_to_idx

    @property
    def idx_to_id(self):
        if not self._idx_to_id:
            self.generate_indices()
        return self._idx_to_id

    @property
    def ids(self):
        if not self._ids:
            self.generate_indices()
        return self._ids

    @property
    def tasks_by_id(self):
        if not self._tasks_by_id:
            self.generate_indices()
        return self._tasks_by_id

    def task_by_idx(self, idx):
        return self.tasks_by_id[self.idx_to_id[idx]]

    def generate_indices(self):
        self._id_to_idx = {}
        self._idx_to_id = {}
        self._ids = []
        self._tasks_by_id = {}
        idx = 0
        for t in self.tasks:
            if not t.id_ in self._id_to_idx:
                self._id_to_idx[t.id_] = idx
                self._idx_to_id[idx] = t.id_
                self._ids.append(t.id_)
                self._tasks_by_id[t.id_] = t
                idx += 1
            else:
                message = 'You must not use the same task ID twice in your decider_spec'
                raise ValueError(message)

    def graph_from_task_specs(self):
        """First index depends on second index:
        m[id_1][id_2] == 1 => 2 depends on 1
        """
        matrix = [[0] * len(self.id_to_idx) for i in range(len(self.id_to_idx))]
        for t in self.tasks:
            if t.requires:
                for required_task in t.requires:
                    index_task = self.id_to_idx[t.id_]
                    index_required_task = self.id_to_idx[required_task.id_]
                    matrix[index_required_task][index_task] = 1
        matrix = self.transitive_reduction(matrix)
        return matrix

    def transitive_reduction(self, graph):
        size = len(graph)
        for j in range(size):
            for i in range(size):
                if graph[i][j]:
                    for k in range(size):
                        if graph[j][k]:
                            graph[i][k] = 0
        return graph

    def get_first_tasks(self):
        first_tasks_ids = []
        for task_id in self.ids:
            column = self._get_column_of_graph_matrix(task_id)
            if all(e == 0 for e in column):
                first_tasks_ids.append(task_id)

        first_tasks = []
        for task_id in first_tasks_ids:
            first_tasks.append(self.tasks_by_id[task_id])
        return first_tasks

    def get_depending_tasks(self, id_):
        completed_activity_idx = self.id_to_idx[id_]
        tasks_indices = [idx for idx, val in enumerate(self.graph[completed_activity_idx]) if val]
        tasks_ids = [self.idx_to_id[idx] for idx in tasks_indices]
        tasks = [self.tasks_by_id[id_] for id_ in tasks_ids]
        return tasks

    def get_dependencies(self, id_):
        activity_task = self.tasks_by_id[id_]
        if activity_task.requires:
            return activity_task.requires
        else:
            return []

    def outgoing_vertices(self):
        """Activity ids of the outgoing vertices, i.e. activity tasks which do not have depending
        tasks

        Returns
        -------
        list: str
             Activity ids
        """
        outgoing = []
        for idx, row in enumerate(self.graph):
            if all(e == 0 for e in row):
                outgoing.append(self.task_by_idx(idx))
        return outgoing

    def _get_column_of_graph_matrix(self, task_id):
        idx = self.id_to_idx[task_id]
        return [row[idx] for row in self.graph]
