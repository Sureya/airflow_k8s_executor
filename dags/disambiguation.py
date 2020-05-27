from typing import TextIO, List

from ..src.adapters import idds_adapter
from ..src.disambiguator import Disambiguator


def disambiguate_destinations(INPUT_FILE: TextIO) -> None:
    '''Identifies which destinations can and cannot be processed further'''
    disambiguator = Disambiguator(idds_adapter)



class DestinationProcessor(Processor):
    """Processes destination tasks
    """

    def __init__(self, generator: Generator, storage_manager: StorageManager):
        self.generator = generator
        self.executor_size = 32
        self.storage_manager = storage_manager

    def _process(self, task: WorkerTask) -> bool:
        """Process a destination request."""
        logger.info('Processing a destination worker task', task_id=task.id)
        request = task.data['data']
        logger.info(f'request_rows={len(request)}', task_id=task.id)
        # Generate for all the destinations. It uses a naive thread parallel
        # mechanism to just brute force the generation
        futures = []
        invalid_results = []
        keywords = []
        ambig_keywords = []
        adgroups = []
        ambig_adgroups = []
        non_comps = []
        with ThreadPoolExecutor(max_workers=self.executor_size) as pool:
            for pair in request:
                future = pool.submit(
                    self.generator.generate_destination_keywords, pair['id'],
                    pair['feeder'], pair['theme'], task.id)
                futures.append(future)

            for future in as_completed(futures):
                result: KeywordResult = future.result()
                if result.invalid:
                    invalid_results.append({'hcom_id': result.hcom_id,
                                            'feeder': result.feeder,
                                            'theme': result.theme,
                                            'reason': result.reason})
                elif result.ambiguous:
                    # Convert them to the DCS format
                    for row in to_sa360_rows(result):
                        ambig_keywords.append(row)
                    ambig_adgroups.append(AdGroup.from_result(result))
                else:
                    # Convert them to the DCS format
                    for row in to_sa360_rows(result):
                        keywords.append(row)
                    adgroups.append(AdGroup.from_result(result))
                for d in _get_non_comp_keywords(result):
                    non_comps.append(d)
        logger.info(f'invalid_ids={len(invalid_results)}',
                    task_id=task.id)
        logger.info(f'keywords={len(keywords)}', task_id=task.id)
        logger.info(f'ambig_keywords={len(ambig_keywords)}',
                    task_id=task.id)

        # -> TODO: Deduplicate inline created keywords so they aren't the same

        # Do filtering of keywords
        keywords = list(filter(
            lambda k: k['Label'] == 'D_DBT_GEO_TDX_XXX', keywords))
        logger.info('keywords put in dataframe were: {}'.format(keywords))
        ambig_keywords = list(filter(
            lambda k: k['Label'] == 'D_DBT_GEO_TDP_XXX', ambig_keywords))
        # Create keyword dataframes.
        kw_df = _create_keyword_dataframe(keywords)
        ambig_kw_df = _create_keyword_dataframe(
            ambig_keywords)
        # Create ad group dataframes.
        adgroup_df = to_sa360_bulksheet(adgroups, task_id=task.id)
        ambig_adgroup_df = to_sa360_bulksheet(ambig_adgroups, task_id=task.id)
        # Create invalid ID dataframe.
        invalid_df = DataFrame(invalid_results)
        # Create non compliant keyword dataframe.
        non_comp_df = DataFrame(non_comps)
        # Store files.
        timestamp = datetime.today().strftime("%Y%m%d%H%M")
        self.storage_manager.save(task.id, kw_df,
                                  f'sa360_keywords_{timestamp}.csv')
        self.storage_manager.save(task.id, adgroup_df,
                                  f'sa360_adgroups_{timestamp}.csv')
        self.storage_manager.save(task.id, ambig_kw_df,
                                  f'sa360_keywords_ambig_{timestamp}.csv')
        self.storage_manager.save(task.id, ambig_adgroup_df,
                                  f'sa360_adgroups_ambig_{timestamp}.csv')
        self.storage_manager.save(task.id, invalid_df,
                                  f'invalid_ids_{timestamp}.csv')
        self.storage_manager.save(task.id, non_comp_df,
                                  f'non_compliant_keywords_{timestamp}.csv')
        self.generator.remove_sessions()
        self.storage_manager.remove_session()
        return True
