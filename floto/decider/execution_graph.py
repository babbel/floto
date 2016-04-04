import logging
logger = logging.getLogger(__name__)

class ExecutionGraph:
    def __init__(self):
        self.ingoing_vertices = {}
        self.outgoing_vertices = {}

    def add_task(self, id_):
        """Adds task id as node to the graph."""
        if not id_ in self.outgoing_vertices:
            self.outgoing_vertices[id_] = set()
            self.ingoing_vertices[id_] = set()
        else:
            raise ValueError('Task with id {} already exists in graph.'.format(id_))

    def add_dependencies(self, id_, dependencies):
        """Adds the dependencies of task with id_ to the graph."""
        if not id_ in self.ingoing_vertices:
            raise ValueError('No node {} in graph'.format(id_))

        self.ingoing_vertices[id_].update(set(dependencies))

        for d in dependencies:
            if not d in self.outgoing_vertices:
                raise ValueError('No node {} in graph'.format(id_))
            self.outgoing_vertices[d].add(id_)

    def get_requires(self, id_):
        """Returns the set ids of required task."""
        if not id_ in self.ingoing_vertices:
            raise ValueError('No node {} in graph'.format(id_))

        return self.ingoing_vertices[id_]

    def get_depending(self, id_):
        """Returns the set of ids of tasks which require the task with id_."""
        if not id_ in self.outgoing_vertices:
            raise ValueError('No node {} in graph'.format(id_))
        return self.outgoing_vertices[id_]

    def get_outgoing_nodes(self):
        return [node for node,vertices in self.outgoing_vertices.items() if not vertices]

    def get_nodes_zero_in_degree(self):
        return [node for node,vertices in self.ingoing_vertices.items() if not vertices]

    def is_acyclic(self):
        """Returns True if graph does not have cycles.
        Implementation of Kahn's algorithm for cycle check O(n):
        https://en.wikipedia.org/wiki/Topological_sorting
        """
        in_degrees = {k:len(v) for k,v in self.ingoing_vertices.items()}
        nodes_zero_in_degree = set(self.get_nodes_zero_in_degree())

        number_nodes_zero_in_degree = 0 
         
        while nodes_zero_in_degree:                
            u = nodes_zero_in_degree.pop()
            number_nodes_zero_in_degree += 1 
            for v in self.outgoing_vertices[u]:
                in_degrees[v] -= 1
                if in_degrees[v] == 0:
                    nodes_zero_in_degree.add(v)
       
        logger.debug('Graph cycle check: Number of sorted nodes: {}. Number of nodes in graph: {}'.
                format(number_nodes_zero_in_degree, len(self.outgoing_vertices)))

        return number_nodes_zero_in_degree == len(self.outgoing_vertices)
   
