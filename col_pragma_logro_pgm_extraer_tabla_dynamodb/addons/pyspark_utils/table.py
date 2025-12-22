from dataclasses import dataclass


@dataclass
class Table:
    """
    Dataclass for handle:
     :database Database name 
     :table_name Table Name
     :additional_options  Aditional options o paramters for data extraction
    """
    database: str
    table_name: str
    origin: str


@dataclass
class S3File:
    """
    Dataclass for handle:
     :bucket Database name 
     :prefix Table Name
     :origin Aditional options o paramters for data extraction
    """
    bucket: str
    prefix: str
    origin: str
    format: str

    @property
    def s3_uri(self) -> str:
        return (
            "s3://"
            f"{self.bucket}"
            "/"
            f"{self.prefix}"
        )
