import pytest
import floto.decider

@pytest.fixture
def graph():
    return floto.decider.ExecutionGraph()

class TestExecutionGraph:
    def test_add_task(self, graph):
        graph.add_task('task_id')
        assert graph.ingoing_vertices['task_id'] == set()
        assert graph.outgoing_vertices['task_id'] == set()

    def test_add_existing_task(self, graph):
        graph.add_task('id')
        with pytest.raises(ValueError):
            graph.add_task('id')

    def test_add_dependencies(self, graph):
        graph.add_task('id')
        graph.add_task('id2')
        graph.add_task('id3')
        graph.add_dependencies('id', ['id2', 'id2', 'id3'])
        assert graph.get_requires('id') == set(['id2', 'id3'])
        assert graph.get_depending('id2') == set(['id'])

    def test_get_requires_raises(self, graph):
        with pytest.raises(ValueError):
            graph.get_requires('id')

    def test_get_depending_raises(self, graph):
        with pytest.raises(ValueError):
            graph.get_depending('id')

    def test_add_dependencies_raises_missing_id_node(self, graph):
        with pytest.raises(ValueError):
            graph.add_dependencies('id', ['id2'])

    def test_add_dependencies_raises_missing_dependency_node(self, graph):
        graph.add_task('id')
        with pytest.raises(ValueError):
            graph.add_dependencies('id', ['id2'])

    def test_is_acyclic_trivial_graph(self, graph):
        graph.add_task('id')
        assert graph.is_acyclic()

    def test_is_acyclic_cyclic_graph(self, graph):
        graph.add_task('a')
        graph.add_task('b')
        graph.add_task('c')
        graph.add_task('d')
        graph.add_dependencies('b', ['a', 'd'])
        graph.add_dependencies('c', ['b'])
        graph.add_dependencies('d', ['a', 'c'])
        assert not graph.is_acyclic()

    def test_is_acyclic(self, graph):
        graph.add_task('a')
        graph.add_task('b')
        graph.add_task('c')
        graph.add_task('d')
        graph.add_dependencies('b', ['a', 'd'])
        graph.add_dependencies('c', ['b'])
        graph.add_dependencies('d', ['a'])
        assert graph.is_acyclic()




