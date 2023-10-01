# notion-exporter: Export Notion pages to Markdown

This python package allows you to easily export your Notion pages to Markdown by providing a Notion API token.

Given that the Notion API is subject to some [rate limits](https://developers.notion.com/reference/request-limits),
this tool will automatically retry failed requests and wait for the rate limit to reset before retrying. This is
especially useful when exporting a large number of pages. Furthermore, this package uses `asyncio` to make requests in
parallel, which can significantly speed up the export process.

## Installation

```bash
pip install notion-exporter
```

## Usage

To use this package, you will need a Notion API token. You can follow the steps outlined in the [Notion documentation](https://developers.notion.com/docs/create-a-notion-integration#create-your-integration-in-notion) 
to create a new Notion integration, connect it to your pages, and obtain your API token.

```python
from notion_exporter import NotionExporter

exporter = NotionExporter(notion_token="<your-token>")
exported_pages = exporter.export_pages(page_ids=["<list-of-page-ids>"])

# exported_pages will be a dictionary where the keys are the page ids and 
# the values are the page content in markdown format
```

The `NotionExporter` class takes the following arguments:
- `notion_token`: Your Notion API token. You can find information on how to get an API token in [Notion's documentation](https://developers.notion.com/docs/create-a-notion-integration)
- `export_child_pages`: Whether to recursively export all child pages of the provided page ids. Defaults to `False`.
- `extract_page_metadata`: Whether to extract metadata from the page and add it as a frontmatter to the markdown. 
                           Extracted metadata includes title, author, path, URL, last editor, and last editing time of 
                           the page. Defaults to `False`.
- `exclude_title_containing`: If specified, pages with titles containing this string will be excluded. This might be
                              useful for example to exclude pages that are archived. Defaults to `None`.

The `NotionExporter.export_pages` method takes the following arguments:
- `page_ids`: A list of page ids to export. If `export_child_pages` is `True`, all child pages of these pages will be
              exported as well.
- `database_ids`: A list of database ids to export. If `export_child_pages` is `True`, all pages in these databases
                  will be exported as well.
- `ids_to_exclude`: A list of page ids to exclude when recursively exporting child pages. If an excluded page is
                    encountered, its child pages will not be exported either.
