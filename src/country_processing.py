# in-built
from os.path import join
from argparse import ArgumentParser
import logging

# 3rd party
import boto3
from pandas import DataFrame
from pycountry import countries
from requests import get

logging.basicConfig(level=logging.INFO)


class S3Utils:
    def __init__(self, bucket_name):
        logging.info("Initialising S3 Bucket")
        self.bucket_name = bucket_name
        self.session = boto3.Session()
        self.s3 = self.session.resource('s3')

    def read_file(self, key):
        logging.info(f"Reading {key}")
        s3_object = self.s3.Object(self.bucket_name, key)
        content = s3_object.get()['Body'].read().decode("utf-8")
        return content

    def write_file(self, content, key):
        logging.info(f"Writing {key}")
        s3_object = self.s3.Object(self.bucket_name, key)
        s3_object.put(Body=content)
        logging.info(f"file available at {self.bucket_name}/{s3_object.key}")
        return True


class CountryProcessor(S3Utils):
    @staticmethod
    def _parse_values(s3_file_path):
        splits = s3_file_path.split('/')
        bucket_name = splits[0]
        run_id = splits[2]
        folder_prefix = join(*splits[1:3])
        s3_key = join(*splits[1:])
        logging.info((bucket_name, run_id, folder_prefix, s3_key))
        return bucket_name, run_id, folder_prefix, s3_key

    def __init__(self, s3_file_name, task_name):
        self.task_mapping = {
            'apply_lower': self.apply_lower,
            'apply_code': self.apply_code,
            'apply_currency': self.apply_currency
        }

        logging.info(f'{task_name} available')

        self.task_name = task_name
        self.s3_file_name = s3_file_name

        self.bucket_name, self.run_id, self.folder_prefix, self.s3_key = \
            self._parse_values(s3_file_path=s3_file_name)

        self.output_s3_key = join(self.folder_prefix, self.task_name,
                                  'output.csv')

        super().__init__(bucket_name=self.bucket_name)
        file_content = super().read_file(key=self.s3_key)
        table_content = [x.split(',') for x in file_content.splitlines()]
        self.data = DataFrame(data=table_content[1:],
                              columns=table_content[0])

        self.task_mapping[self.task_name]()

    @staticmethod
    def _get_country_code(country_name):
        try:
            return countries.get(name=country_name.capitalize()).alpha_3
        except Exception as e:
            return "NA"

    @staticmethod
    def _get_currency_code(country_name):
        try:
            url = f"https://restcountries.eu/rest/v2/name/{country_name}" \
                  f"?fullText=true"
            response = get(url=url)
            result = response.json()

            if isinstance(result, list):
                return result[0]['currencies'][0]['code']

            else:
                return None

        except Exception as e:
            return "NA"

    def apply_lower(self):
        self.data["country"] = self.data["country"].apply(lambda x: x.lower())
        super().write_file(key=self.output_s3_key, content=self.data.to_csv())

    def apply_code(self):
        self.data["country_code"] = self.data["country"].apply(
            self._get_country_code)

        super().write_file(key=self.output_s3_key, content=self.data.to_csv())

    def apply_currency(self):
        self.data["currency_code"] = self.data["country"].apply(
            self._get_currency_code)

        super().write_file(key=self.output_s3_key, content=self.data.to_csv())


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('--s3_file', required=True)
    parser.add_argument('--task_name', required=True,
                        choices=['apply_lower', 'apply_code',
                                 'apply_currency'])
    args = parser.parse_args()
    process = CountryProcessor(s3_file_name=args.s3_file,
                               task_name=args.task_name)
