import pytest
import floto.api
import unittest.mock
import botocore.client

@pytest.fixture(scope='module')
def domains():
    swf = floto.api.Swf()
    return floto.api.Domains(swf)

class TestDomains:
    def test_names(self, domains, mocker):
        mocker.patch('floto.api.Domains._domains', return_value=[{'name':'d'}])
        domain_names = list(domains.names('REGISTERED'))
        domains._domains.assert_called_once_with('REGISTERED')
        assert domain_names[0] == 'd'

    def test_registered_names(self, domains, mocker):
        mocker.patch('floto.api.Domains.names')
        domains.registered_names()
        domains.names.assert_called_once_with(**{'registration_status':'REGISTERED'})
        
    def test_deprecated_names(self, domains, mocker):
        mocker.patch('floto.api.Domains.names')
        domains.deprecated_names()
        domains.names.assert_called_once_with(**{'registration_status':'DEPRECATED'})
        
    def test_domain_exists(self, domains, mocker):
        mocker.patch('floto.api.Domains.registered_names', return_value=['n1', 'n2'])
        assert domains.domain_exists('n1')
        assert not domains.domain_exists('foo')

    def test_domain_does_not_exist_w_empty_domain_list(self, domains, mocker):
        mocker.patch('floto.api.Domains.registered_names', return_value=[])
        assert not domains.domain_exists('foo')

    @pytest.mark.parametrize('args, private_args',[
            ({'name':'n'}, {'name':'n', 'description':'', 'retention_period':'7'}),
            ({'name':'n', 'description':'d'}, {'name':'n', 'description':'d', 'retention_period':'7'}),
            ({'name':'n', 'retention_period':'3'}, {'name':'n', 'description':'', 'retention_period':'3'})
            ])
    def test_register_domain(self, args, private_args, domains, mocker):
        mocker.patch('floto.api.Domains._register_domain')

        domains.register_domain(**args)
        domains._register_domain.assert_called_once_with(**private_args)

class Client_Mock(object):
    def register_domain(self, **args):
        pass

class TestDomainsAPICalls:
    import botocore.exceptions

    def test_register_domain_args(self, domains, mocker):
        mocker.patch('floto.api.Swf.client', new_callable=unittest.mock.PropertyMock, 
                return_value=Client_Mock())
        Client_Mock.register_domain = unittest.mock.Mock()
        domains._register_domain(name='domain_name', description='description', 
                retention_period='NONE')
        expected_args = {'name':'domain_name',
                         'description':'description',
                         'workflowExecutionRetentionPeriodInDays':'NONE'}
        domains.swf.client.register_domain.assert_called_once_with(**expected_args)
        

    def test_register_domain_error(self, domains, mocker):
        error_response = {'Error':{'Code':'MyFault'}}
        client_error = botocore.exceptions.ClientError(error_response=error_response,
                operation_name="op_name")

        mocker.patch('floto.api.Swf.client', new_callable=unittest.mock.PropertyMock, 
                return_value=Client_Mock())

        Client_Mock.register_domain = unittest.mock.Mock(side_effect=client_error)

        with pytest.raises(botocore.exceptions.ClientError):
            domains._register_domain(name='domain_name', description='description', retention_period='NONE')
