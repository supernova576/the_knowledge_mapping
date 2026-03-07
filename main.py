from src.DocsParser import DocsParser

import traceback

def print_helper_banner() -> None:
    help_message = """
    Usage: python3 main.py [options]

    Help:
    --help                  Display this help message

    Options:
    --run                   Run full scan of docs (also updates old entries)
    
    --get-by-id [id]        Gets an entry by id
    --get-by-name [name]    Gets an entry by file-name
    --get-incompliant       Gets all incompliant files

    --delete-by-id          Deletes an entry by id
    --delete-by-name        Deletes an entry by file-name
    --delete-all            Deletes ALL DB-Entries! Be careful!!

    --export-result         Exports Results to a .MD-File
    """
    print(help_message)

def main():
    try:
        docs_parser_obj = DocsParser()

        docs_parser_obj.parse_and_add_ALL_docs_to_db()
    except Exception:
        print(traceback.format_exc())

if __name__ == "__main__":
    main()