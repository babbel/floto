import pytest
import floto.decider
from floto.specs import ActivityTask, Timer

class TestExecutionGraph():
    def test_size_matrix_graph_from_task_spec(self):
        tasks = [ActivityTask(activity_id='t1:1', name='t1', version='1'),
                 ActivityTask(activity_id='t2:1', name='t2', version='1')]
        g = floto.decider.ExecutionGraph(activity_tasks=tasks)
        matrix = g.graph_from_task_specs()
        assert len(matrix) == 2
        assert len(matrix[0]) == 2
        assert matrix[0][0] == 0

    def test_graph_from_task_specs(self):
        t1 = ActivityTask(activity_id='t1:1', name='t1', version='1')
        t2 = Timer(id_='t2', requires=[t1])
        g = floto.decider.ExecutionGraph(activity_tasks=[t1,t2])
        t1_idx = g.id_to_idx['t1:1']
        t2_idx = g.id_to_idx['t2']

        assert g.graph[t1_idx][t2_idx] == 1
        assert g.graph[t1_idx][t1_idx] == 0
        assert g.graph[t2_idx][t1_idx] == 0
        assert g.graph[t2_idx][t2_idx] == 0

    def test_transitive_reduction(self):
        t1 = ActivityTask(activity_id='t1:1', name='t1', version='1')
        t2 = ActivityTask(activity_id='t2:1', name='t2', version='1', requires=[t1])
        t3 = ActivityTask(activity_id='t3:1', name='t3', version='1', requires=[t1, t2])
        
        g = floto.decider.ExecutionGraph(activity_tasks=[t1,t2,t3])
        t1_idx = g.id_to_idx['t1:1']
        t2_idx = g.id_to_idx['t2:1']
        t3_idx = g.id_to_idx['t3:1']

        assert g.graph[t1_idx][t2_idx] == 1
        assert g.graph[t2_idx][t3_idx] == 1
        assert g.graph[t1_idx][t3_idx] == 0

    def test_id_to_idx(self):
        tasks = [ActivityTask(activity_id='t1:1', name='t1', version='1'),
                 ActivityTask(activity_id='t2:1', name='t2', version='1')]
        g = floto.decider.ExecutionGraph(activity_tasks=tasks)
        assert g.id_to_idx['t1:1'] == 0
        assert g.id_to_idx['t2:1'] == 1

    def test_id_to_idx_raises_for_duplicate_activity_ids(self):
        tasks = [ActivityTask(activity_id='t1:1', name='t1', version='1'),
                 ActivityTask(activity_id='t1:1', name='t2', version='1')]
        g = floto.decider.ExecutionGraph(activity_tasks=tasks)
        with pytest.raises(ValueError):
            g.id_to_idx['t1:1']

    def test_idx_to_id(self):
        tasks = [ActivityTask(activity_id='t1:1', name='t1', version='1')]
        g = floto.decider.ExecutionGraph(activity_tasks=tasks)
        assert g.idx_to_id[0] == 't1:1'

    def test_ids(self):
        tasks = [ActivityTask(activity_id='t1:1', name='t1', version='1')]
        g = floto.decider.ExecutionGraph(activity_tasks=tasks)
        assert g.ids == ['t1:1']

    def test_tasks_by_id(self):
        tasks = [ActivityTask(activity_id='t1:1', name='t1', version='1')]
        g = floto.decider.ExecutionGraph(activity_tasks=tasks)
        assert g.tasks_by_id['t1:1'].name == 't1'

    def test_get_column_of_graph_matrix(self):
        t1 = ActivityTask(activity_id='t1:1', name='t1', version='1')
        t2 = ActivityTask(activity_id='t2:1', name='t2', version='1', requires=[t1])
        tasks = [t1,t2]
        g = floto.decider.ExecutionGraph(activity_tasks=tasks)
        assert g._get_column_of_graph_matrix('t1:1') == [0,0]
        assert g._get_column_of_graph_matrix('t2:1') == [1,0]

    def test_get_first_tasks(self):
        t1 = ActivityTask(activity_id='t1:1', name='t1', version='1')
        t2 = ActivityTask(activity_id='t2:1', name='t2', version='1', requires=[t1])
        g = floto.decider.ExecutionGraph(activity_tasks=[t1,t2])
        assert len(g.get_first_tasks()) == 1 
        assert g.get_first_tasks()[0].id_ == 't1:1' 

    def test_get_depending_tasks(self):
        t1 = ActivityTask(activity_id='t1:1', name='t1', version='1')
        t2 = ActivityTask(activity_id='t2:1', name='t2', version='1', requires=[t1])
        g = floto.decider.ExecutionGraph(activity_tasks=[t1,t2])
        depending_tasks = g.get_depending_tasks('t1:1')
        assert len(depending_tasks) == 1
        assert depending_tasks[0].id_ == 't2:1'

    def test_get_dependencies(self):
        t1 = ActivityTask(activity_id='t1:1', name='t1', version='1')
        t2 = ActivityTask(activity_id='t2:1', name='t2', version='1', requires=[t1])
        g = floto.decider.ExecutionGraph(activity_tasks=[t1,t2])
        dependencies = g.get_dependencies('t2:1')
        assert len(dependencies) == 1
        assert dependencies[0].id_ == 't1:1'

    def test_outgoing_vertices(self):
        t1 = ActivityTask(activity_id='t1:1', name='t1', version='1')
        t2 = ActivityTask(activity_id='t2:1', name='t2', version='1', requires=[t1])
        t3 = ActivityTask(activity_id='t3:1', name='t3', version='1', requires=[t2])
        t4 = ActivityTask(activity_id='t4:1', name='t4', version='1')
        g = floto.decider.ExecutionGraph(activity_tasks=[t1,t2,t3,t4])
        outgoing = g.outgoing_vertices()
        assert len(outgoing) == 2
        assert set([e.id_ for e in outgoing]) == set(['t3:1', 't4:1'])


