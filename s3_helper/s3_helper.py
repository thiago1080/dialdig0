from loguru import logger
import pandas as pd
import boto3
import io

class S3Helper:
    def __init__(self):
        self.s3 = None

    def client(self, aws_access_key_id = None, aws_secret_access_key = None, aws_session_token = None):
        self.s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, aws_session_token = aws_session_token)


    def load_s3(self, bucket_name = None, file_key = None):
        """
        Load a Pandas DataFrame from an S3 bucket and return it.
        """
        try:
            # create S3 client with credentials
            obj = self.s3.get_object(Bucket=bucket_name, Key=file_key)
            data = obj['Body'].read()
            data_io = io.BytesIO(data)
            logger.success(f'Função realizada com sucesso')
            return data_io
        except Exception as e:
            logger.error(e)
            raise e
    
    def data2df(self, data, type = 'parquet'):
        """_summary_

        Args:
            type (str, optional): _description_. Defaults to 'parquet'.

        Returns:
            df : pd.DataFrame
        """
        # create S3 client with credentials
        
        try: 
            if type == 'parquet':
                df = pd.read_parquet(data)
            elif type == 'csv':
                df = pd.read_csv(data)
            elif type == 'json':
                df = pd.read_json(data)
            elif type == 'excel':
                df = pd.read_excel(data)
            else:
                raise Exception('Tipo de arquivo não suportado')
            logger.success(f'Sucesso. Dataframe contém {df.shape[0]} linhas')
        except Exception as e:
            logger.error(e)
        return df

    def load_df(self, bucket_name = None, file_key = None, type = 'parquet'):
        data = self.load_s3(bucket_name, file_key)
        df = self.data2df(data, type = type)
        return df


def parse_s3(bucket_name: str, file_name: str, s3_prefix = 's3', site_name = 's3-website-ap-southeast-2.amazonaws.com'):
    return f'{s3_prefix}:/{bucket_name}.{site_name}/{file_name}'

def load_df_s3_2(bucket_name, file_key):
    """
    Load a Pandas DataFrame from an S3 bucket and return it.
    """
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    return  pd.read_parquet(data)

