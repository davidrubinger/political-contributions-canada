from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from requests import get
from io import BytesIO
from zipfile import ZipFile

class UnzipURLOperator(BaseOperator):

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
