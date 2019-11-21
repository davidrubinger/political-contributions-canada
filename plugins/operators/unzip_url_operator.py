from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from requests import get
from io import BytesIO
from zipfile import ZipFile

class UnzipURLOperator(BaseOperator):
    """
    Unzips and downloads file from URL
    
    :param url: URL where zip file resides
    :type url: str
    :param unzip_dir: directory to unzip and download files to
    :type unzip_dir: str
    """
    
    @apply_defaults
    def __init__(self,
                 url,
                 unzip_dir="",
                 *args, **kwargs):
        
        super(UnzipURLOperator, self).__init__(*args, **kwargs)
        self.url = url
        self.unzip_dir = unzip_dir
    
    def execute(self, context):
        response = get(self.url)
        zip_file = ZipFile(BytesIO(response.content))
        zip_file.extractall(self.unzip_dir)
