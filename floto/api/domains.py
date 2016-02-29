import logging

import botocore.exceptions

logger = logging.getLogger(__name__)


class Domains(object):
    def __init__(self, swf):
        self.swf = swf

    def registered_names(self):
        """Names of registered domains.
        Yields
        ------
        str:
            Name of registered domain
        """
        return self.names(registration_status='REGISTERED')

    def deprecated_names(self):
        """Names of deprecated domains.
        Yields
        ------
        str:
            Name of deprecated domain
        """
        return self.names(registration_status='DEPRECATED')

    def names(self, registration_status):
        """Generator

        Parameters
        ----------
        registration_status: str
            REGISTERED|DEPRECATED

        Yields
        -----
        str:
            Name of the domain
        """
        for domain in self._domains(registration_status):
            yield domain['name']

    def domain_exists(self, name):
        """Check if domain <name> is already registered.

        Parameters
        ----------
        name: str

        Returns
        -------
        True if domain exists, False if not.
        """
        return [True for registered in self.registered_names() if registered == name] != []

    def register_domain(self, name, description='', retention_period='7'):
        """Registers a new domain. Does nothing if domain already exists.

        Attention, attention, please!!!
        Once a domain is deprecated it can not be reanimated. It is not possible to register a new
        domain with the same name. Neither is it possible do remove deprecated domains completely.
        Be very careful with registration and deprecation of domains in order not to pollute the 
        history of registered domains too much.

        Parameters
        ----------
        name : str
            Domain name
        description: Optional[str]
            Description of domain
        retention_period: Optional[str]
            The duration (in days) that records and histories of workflow executions on the domain
            are kept. If set to NONE the execution history is not retained. 
            The maximum period is 90 days.
            As soon as the workflow execution completes, the execution record and its history are 
            deleted.
        """
        self._register_domain(name=name, description=description, retention_period=retention_period)

    def deprecate_domain(self, name):
        """Deprecates the domain.
        Attention, attention, please!!!
        Once a domain is deprecated it can not be reanimated. It is not possible to register a new
        domain with the same name. Neither is it possible do remove deprecated domains completely.
        Be very careful with registration and deprecation of domains in order not to pollute the 
        history of registered domains too much.
        
        https://forums.aws.amazon.com/thread.jspa?threadID=91629

        Parameters
        ----------
        name : str
        """
        self._deprecate_domain(name=name)

    #############################################################################################
    #### Private functions interfacing Amazon's SWF service                                   ###
    #### See http://boto3.readthedocs.org/en/latest/reference/services/swf.html for reference ###
    #############################################################################################

    def _domains(self, registration_status):
        """Retrieves domain information from SWF.

        Parameter
        ----------
        registration_status: str
            REGISTERED|DEPRECATED

        Yields
        -----
        dict:
            domain information (description, name, status)
        """
        paginator = self.swf.client.get_paginator('list_domains')
        pages = paginator.paginate(registrationStatus=registration_status)
        for page in pages:
            for domain in page['domainInfos']:
                yield domain

    def _register_domain(self, name, description, retention_period):
        try:
            self.swf.client.register_domain(name=name, description=description,
                                            workflowExecutionRetentionPeriodInDays=retention_period)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'DomainAlreadyExistsFault':
                logger.warning('Failed to register already existing domain {}.'.format(name))
            else:
                logger.error(e)
                raise e

    def _deprecate_domain(self, name):
        self.swf.client.deprecate_domain(name=name)
