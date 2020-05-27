import csv
import io


class IddsAdapter(object):
    '''Implements the Disambiguation interface.
    Talks to HCOM's IDDS to check destination name uniqueness.'''

    def __init__(self, base_url: str, timeout: float = TIMEOUT):
        self.idds_api_base_url = base_url
        self.timeout = timeout

    def is_destination_name_ambiguous(self, row: str) -> bool:
        """Determine if there's a more important destination with the same name in our database.

        Args:
            hcom_id: Hcom id of the id targeted
            feeder: The feeder that the keyword's targeting
            task_id: Optional task id for logging

        Returns:
            True if ID is ambiguous.
                - ambiguous: hcom_id is in ambiguous_performance view and
                  another ID is the popular ID
            False if ID is not ambiguous.
                - non-ambiguous: hcom_id is in ambiguous_performance view and
                  its ID is the popular ID
                - unique: not in the ambiguous table
        """
        logger.info(
            f'Destination disambiguate hcom_id={hcom_id} feeder={feeder}',
            task_id=task_id)
        hcom_id, feeder = read_and_validate(row)
        status = self.check_destinaton_in_idds(hcom_id, feeder)
        return status == 'ambiguous'

    """Can be moved somewhere else, e.g. some ValidateFile interface/function"""
    def read_and_validate(row: str) -> dict:
        fieldnames = ['hcom_id', 'feeder']
        reader = csv.reader(io.StringIO(row), fieldnames=fieldnames)
        validation_dict = next(reader)
        return validation_dict['hcom_id'], validation_dict['feeder']

    def check_destination_in_idds(self, hcom_id: int, feeder: str) -> str:
        """Sends the destination disambiguation request to IDDS
        Return:
            status string: 'ambiguous' or 'non-ambiguous' or 'unique'"""
        body = {'hcom_id': hcom_id, 'feeder': feeder}
        logger.info('Payload sent to IDDS was {}'.format(body))
        return self._request('api/disambiguate/destination', body, 'status')

    def _request(self, uri: str, body: dict, key: Optional[str]):
        """Creates the request and parses response"""
        url = urljoin(self.idds_api_base_url, uri)
        response = self._handle_request(url, body)
        logger.info('Response from IDDS was {}'.format(response))
        return response[key] if key else response

    def _handle_request(self, url: str, body: dict) -> dict:
        """Actually sends the request and returns the body"""
        try:
            r = requests.post(url, json=body, timeout=self.timeout)
            return r.json()
        except JSONDecodeError as e:
            logger.error(f'IDDS error={body} status_code={r.status_code}')
            raise e
