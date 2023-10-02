import boto3
import tempfile
import gzip


def main():
    aws_access_key = 'AKIAU4EQQIVLVAX74XW2'
    aws_secret_key = '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'
    bucket_name = 'commoncrawl'
    file_key = 'crawl-data/CC-MAIN-2022-05/wet.paths.gz'
    temp_folder = tempfile.TemporaryDirectory()
    new_key = ''

    s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

    try:
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        gzipped_content = response['Body'].read()
        new_key = gzip.decompress(gzipped_content).decode('utf-8').split('\n', 1)[0].strip()

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    try:
        local_file_path = temp_folder.name + '/' + new_key.split('/')[-1]
        s3.download_file(bucket_name, new_key, local_file_path)
        with gzip.open(local_file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                # Process 'line' here
                print(line.strip())

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
