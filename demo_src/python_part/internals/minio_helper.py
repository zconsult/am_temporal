from minio import Minio

import pandas as pd
import xarray as xr

from .config_helper import get_config_part
from .config_entries import *


class MinioDumbHelper:
    def __init__(self):
        minio_info = get_config_part(MINIO_INFO)

        endpoint = minio_info["endpoint"]
        access_key = minio_info["access_key"]
        secret_key = minio_info["secret_key"]

        self.client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=False)

        self.bucket = minio_info["bucket_name"]

        self.getf = self.client.fget_object
        self.putf = self.client.fput_object

    def upload(self, file_name: str, object_name: str) -> None:
        self.putf(bucket_name=self.bucket, object_name=object_name, file_path=file_name)

    def download(self, file_name: str, object_name: str) -> None:
        self.getf(bucket_name=self.bucket, object_name=object_name, file_path=file_name)

    def read_pandas(self, conf: dict) -> pd.DataFrame:
        input_minio_object = conf['input_minio_object']
        input_download_file_name = conf['download_file_name']

        self.download(file_name=input_download_file_name, object_name=input_minio_object)

        data = pd.read_parquet(input_download_file_name, engine='pyarrow')
        return data

    def write_xarray(self, x_array: xr.DataArray, conf: dict) -> str:

        upload_file_name = conf['upload_file_name']
        output_minio_object = conf['output_minio_object']

        x_array.to_netcdf(path=upload_file_name, mode='w')

        self.upload(file_name=upload_file_name, object_name=output_minio_object)

        return output_minio_object

    def read_xarray(self, conf: dict) -> xr.DataArray:
        input_minio_object = conf['input_minio_object']
        input_download_file_name = conf['download_file_name']

        self.download(file_name=input_download_file_name, object_name=input_minio_object)

        x_array = xr.open_dataarray(input_download_file_name)
        return x_array

