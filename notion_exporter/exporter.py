from typing import Optional
import asyncio
import logging

from notion_client import AsyncClient as NotionClient, APIResponseError
from notion_client import Client
from notion_client.helpers import async_collect_paginated_api
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from notion_exporter.block_converter import BlockConverter
from notion_exporter.property_converter import PropertyConverter
from notion_exporter.retry_utils import is_rate_limit_exception, wait_for_retry_after_header, is_unavailable_exception


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)


class NotionExporter:
    """
    Export Notion pages and databases to markdown files.
    """

    def __init__(
        self,
        notion_token: str,
        export_child_pages: bool = False,
        extract_page_metadata: bool = False,
        exclude_title_containing: Optional[str] = None,
    ):
        """
        :param notion_token: Notion API token.
        :param export_child_pages: Whether to export child pages. Default: True.
        :param extract_page_metadata: Whether to extract page metadata. Default: False.
        :param exclude_title_containing: If specified, pages with titles containing this string will be excluded.

        """
        self.notion = NotionClient(auth=notion_token)
        self.sync_notion = Client(auth=notion_token)
        self.export_child_pages = export_child_pages
        self.extract_page_metadata = extract_page_metadata
        self.exclude_title_containing = exclude_title_containing
        self.block_converter = BlockConverter()
        self.property_converter = PropertyConverter(self)

    def export_pages(
        self,
        page_ids: Optional[list[str]] = None,
        database_ids: Optional[list[str]] = None,
        ids_to_exclude: Optional[list[str]] = None,
    ) -> dict[str, str]:
        """
        Export pages and databases to markdown files.

        :param page_ids: List of page IDs to export.
        :param database_ids: List of database IDs to export.
        :param ids_to_exclude: List of IDs to ignore.
        """
        if page_ids is None and database_ids is None:
            raise ValueError("Either page_ids or database_ids must be specified.")

        if ids_to_exclude is None:
            ids_to_exclude = set()
        if page_ids is None:
            page_ids = set()
        if database_ids is None:
            database_ids = set()

        page_ids = set(map(self._normalize_id, page_ids))
        database_ids = set(map(self._normalize_id, database_ids))
        ids_to_exclude = set(map(self._normalize_id, ids_to_exclude))

        page_ids = page_ids - ids_to_exclude
        database_ids = database_ids - ids_to_exclude
        extracted_pages, _, _ = asyncio.run(
            self._async_export_pages(page_ids=page_ids, database_ids=database_ids, ids_to_exclude=ids_to_exclude)
        )

        return extracted_pages

    async def _async_export_pages(
        self,
        page_ids: set[str],
        database_ids: set[str],
        ids_to_exclude: Optional[set] = None,
        parent_page_ids: Optional[dict] = None,
        page_paths: Optional[dict] = None,
    ):
        if ids_to_exclude is None:
            ids_to_exclude = set()
        if page_paths is None:
            page_paths = {}
        if parent_page_ids is None:
            parent_page_ids = {}

        page_ids -= ids_to_exclude
        database_ids -= ids_to_exclude
        ids_to_exclude.update(page_ids)
        ids_to_exclude.update(database_ids)

        extracted_pages = {}
        child_pages = set()
        child_databases = set()
        if page_ids:
            for page_id in page_ids:
                logger.info(f"Fetching page {page_id}.")
            page_meta_tasks = [self._get_page_meta(page_id) for page_id in page_ids]
            page_content_tasks = [self._get_block_content(page_id) for page_id in page_ids]
            page_details_results = await asyncio.gather(*page_meta_tasks)
            page_content_results = await asyncio.gather(*page_content_tasks)
            ids_to_exclude.update(page["page_id"] for page in page_details_results)

            for page_details, (markdown, child_page_ids, child_database_ids) in zip(
                page_details_results, page_content_results
            ):
                if (
                    self.exclude_title_containing
                    and self.exclude_title_containing.lower() in page_details.get("title", "").lower()
                ):
                    continue
                for child_page_id in child_page_ids:
                    parent_page_ids[child_page_id] = page_details["page_id"]
                for child_database_id in child_database_ids:
                    parent_page_ids[child_database_id] = page_details["page_id"]
                front_matter = self._get_page_front_matter(page_details, page_paths, parent_page_ids=parent_page_ids)
                markdown = "\n".join(markdown)
                extracted_pages[page_details["page_id"]] = f"{front_matter}\n{markdown}"
                child_pages.update(child_page_ids)
                child_databases.update(child_database_ids)

        if database_ids:
            for database_id in database_ids:
                logger.info(f"Fetching database {database_id}.")
            database_meta_tasks = [self._get_database_meta(database_id) for database_id in database_ids]
            database_content_tasks = [self._get_database_content(database_id) for database_id in database_ids]
            database_content_results = await asyncio.gather(*database_content_tasks)
            database_details_results = await asyncio.gather(*database_meta_tasks)
            ids_to_exclude.update(database["page_id"] for database in database_details_results)

            for db_details, (markdown, entry_ids) in zip(database_details_results, database_content_results):
                if (
                    self.exclude_title_containing
                    and self.exclude_title_containing.lower() in db_details.get("title", "").lower()
                ):
                    continue
                for entry_id in entry_ids:
                    parent_page_ids[entry_id] = db_details["page_id"]
                front_matter = self._get_page_front_matter(db_details, page_paths, parent_page_ids)
                extracted_pages[db_details["page_id"]] = f"{front_matter}\n{markdown}"
                child_pages.update(entry_ids)

        if self.export_child_pages and (child_pages or child_databases):
            extracted_child_pages, _, _ = await self._async_export_pages(
                page_ids=child_pages,
                database_ids=child_databases,
                ids_to_exclude=ids_to_exclude,
                parent_page_ids=parent_page_ids,
                page_paths=page_paths,
            )
            extracted_pages.update(extracted_child_pages)

        return extracted_pages, child_pages, child_databases

    async def _get_block_content(
        self,
        block_id: str,
        parent_is_list_item: bool = False,
        indent_level: int = 0,
        child_page_ids: Optional[set] = None,
        child_database_ids: Optional[set] = None,
    ) -> tuple[list[str], set, set]:
        if child_page_ids is None:
            child_page_ids = set()
        if child_database_ids is None:
            child_database_ids = set()

        blocks = await self._get_child_blocks(block_id)
        if blocks and blocks[0]["type"] == "table_row":
            blocks = self._add_delimiter_to_table(blocks)
        markdown_blocks_with_child_tasks = []
        child_tasks = []
        for block in blocks:
            is_child_page = block["type"] == "child_page"
            is_child_database = block["type"] == "child_database"
            is_list_item = block["type"] in ["bulleted_list_item", "numbered_list_item", "to_do", "toggle"]

            markdown_block = self.block_converter.convert_block(
                block=block,
                indent=parent_is_list_item and is_list_item,
                indent_level=indent_level,
            )
            if is_child_page:
                child_page_ids.add(block["id"])
            if is_child_database:
                child_database_ids.add(block["id"])

            if block["has_children"] and not (is_child_page or is_child_database):
                task = asyncio.create_task(
                    self._get_block_content(
                        block_id=block["id"],
                        parent_is_list_item=is_list_item,
                        indent_level=indent_level + 1,
                        child_page_ids=child_page_ids,
                    )
                )
            else:
                task = asyncio.Future()
                task.set_result(([], set(), set()))

            markdown_blocks_with_child_tasks.append((markdown_block, task))
            child_tasks.append(task)

        # Start all tasks concurrently
        await asyncio.gather(*child_tasks)

        markdown_blocks = []
        # Await the tasks and extend the markdown_blocks
        for markdown_block, task in markdown_blocks_with_child_tasks:
            child_blocks, _, _ = task.result()
            markdown_blocks.append(markdown_block)
            markdown_blocks.extend(child_blocks)

        return markdown_blocks, child_page_ids, child_database_ids

    @retry(
        retry=(
            retry_if_exception(predicate=is_rate_limit_exception)
            | retry_if_exception(predicate=is_unavailable_exception)
        ),
        wait=wait_for_retry_after_header(fallback=wait_exponential()),
        stop=stop_after_attempt(3),
    )
    async def _get_database_content(self, database_id: str) -> tuple[str, set[str]]:
        try:
            database = await self.notion.databases.retrieve(database_id)
            database_entries = await async_collect_paginated_api(self.notion.databases.query, database_id=database_id)
            entry_ids = set()

            description = database["description"][0]["plain_text"] if database["description"] else ""

            title_column = [col_name for col_name, col in database["properties"].items() if col["type"] == "title"][0]
            db_page_header = f"{description}\n\n"
            table_header = f"|{title_column}|{'|'.join([prop['name'] for prop in database['properties'].values() if prop['name'] != title_column])}|\n"
            table_header += "|" + "---|" * (len(database["properties"])) + "\n"
            table_body = ""
            for entry in database_entries:
                table_body += f"|{self.property_converter.convert_property(entry['properties'][title_column])}|"
                table_body += "|".join(
                    [
                        self.property_converter.convert_property(prop)
                        for prop_name, prop in entry["properties"].items()
                        if prop_name != title_column
                    ]
                )
                table_body += "|\n"
                entry_ids.add(entry["id"])

            db_page = f"{db_page_header}{table_header}{table_body}"
        except APIResponseError as exc:
            # Database is not available via API, might be a linked database
            if exc.code in ["object_not_found", "validation_error"]:
                db_page = ""
                entry_ids = set()
            else:
                raise exc

        return db_page, entry_ids

    @retry(
        retry=(
            retry_if_exception(predicate=is_rate_limit_exception)
            | retry_if_exception(predicate=is_unavailable_exception)
        ),
        wait=wait_for_retry_after_header(fallback=wait_exponential()),
        stop=stop_after_attempt(3),
    )
    async def _get_child_blocks(self, page_id: str) -> list[dict]:
        try:
            return await async_collect_paginated_api(self.notion.blocks.children.list, block_id=page_id)
        except APIResponseError as exc:
            # Page is not available via API, might be private
            if exc.code == "object_not_found":
                return []
            else:
                raise exc

    @retry(
        retry=(
            retry_if_exception(predicate=is_rate_limit_exception)
            | retry_if_exception(predicate=is_unavailable_exception)
        ),
        wait=wait_for_retry_after_header(fallback=wait_exponential()),
        stop=stop_after_attempt(3),
    )
    async def _get_page_meta(self, page_id: str) -> dict:
        page_object = await self.notion.pages.retrieve(page_id)
        created_by, last_edited_by = await asyncio.gather(
            self._get_user(page_object["created_by"]["id"]), self._get_user(page_object["last_edited_by"]["id"])
        )

        # Database entries don't have an explicit title property, but a title column
        # Also, we extract all properties from the database entry to be able to add them to the markdown page as
        # key-value pairs
        properties = {}
        if page_object["parent"]["type"] == "database_id":
            title = ""
            for prop_name, prop in page_object["properties"].items():
                if prop["type"] == "title":
                    title = prop["title"][0]["plain_text"] if prop["title"] else ""
                properties[prop_name] = self.property_converter.convert_property(prop)

        else:
            title = (
                page_object["properties"]["title"]["title"][0]["plain_text"]
                if page_object["properties"]["title"]["title"]
                else ""
            )

        page_meta = {
            "title": title,
            "url": page_object["url"],
            "created_by": created_by,
            "last_edited_by": last_edited_by,
            "last_edited_time": page_object["last_edited_time"],
            "page_id": page_object["id"],
            "parent_id": page_object["parent"][page_object["parent"]["type"]],
        }
        if properties:
            page_meta["properties"] = properties

        return page_meta

    @retry(
        retry=(
            retry_if_exception(predicate=is_rate_limit_exception)
            | retry_if_exception(predicate=is_unavailable_exception)
        ),
        wait=wait_for_retry_after_header(fallback=wait_exponential()),
        stop=stop_after_attempt(3),
    )
    async def _get_database_meta(self, database_id: str) -> dict:
        try:
            database_object = await self.notion.databases.retrieve(database_id)
            created_by, last_edited_by = await asyncio.gather(
                self._get_user(database_object["created_by"]["id"]),
                self._get_user(database_object["last_edited_by"]["id"]),
            )

            database_meta = {
                "title": database_object["title"][0]["plain_text"] if database_object["title"] else "Untitled",
                "url": database_object["url"],
                "created_by": created_by,
                "last_edited_by": last_edited_by,
                "last_edited_time": database_object["last_edited_time"],
                "page_id": database_object["id"],
                "parent_id": database_object["parent"][database_object["parent"]["type"]],
            }
        except APIResponseError as exc:
            # Database is not available via API, might be a linked database
            if exc.code in ["object_not_found", "validation_error"]:
                database_meta = {
                    "title": "Untitled",
                    "url": "",
                    "created_by": "",
                    "last_edited_by": "",
                    "last_edited_time": "",
                    "page_id": database_id,
                    "parent_id": "",
                }
            else:
                raise exc

        return database_meta

    @retry(
        retry=(
            retry_if_exception(predicate=is_rate_limit_exception)
            | retry_if_exception(predicate=is_unavailable_exception)
        ),
        wait=wait_for_retry_after_header(fallback=wait_exponential()),
        stop=stop_after_attempt(3),
    )
    async def _get_user(self, user_id: str) -> str:
        try:
            user_object = await self.notion.users.retrieve(user_id)
            return user_object["name"]
        except Exception as e:
            if isinstance(e, APIResponseError) and e.code == "rate_limited":
                raise e

        return "Unknown"

    def _get_page_front_matter(self, page_meta: dict, page_paths: dict, parent_page_ids: dict) -> str:
        if page_meta["page_id"] in parent_page_ids:
            parent_page_id = parent_page_ids.pop(page_meta["page_id"])
            current_page_path = page_paths[parent_page_id] + " / " + page_meta["title"]
        else:
            current_page_path = page_meta["title"]
        page_paths[page_meta["page_id"]] = current_page_path

        front_matter = ""
        if self.extract_page_metadata:
            front_matter += "---\n"
            front_matter += f"title: {page_meta['title']}\n"
            # Add quotation marks to avoid issues with colons in page titles
            front_matter += f'path: "{current_page_path}"\n'
            front_matter += f"url: {page_meta['url']}\n"
            front_matter += f"created_by: {page_meta['created_by']}\n"
            front_matter += f"last_edited_by: {page_meta['last_edited_by']}\n"
            front_matter += f"last_edited_time: {page_meta['last_edited_time']}\n"
            front_matter += "---\n\n"

        # Add properties of database entries as key-value pairs
        if "properties" in page_meta:
            for prop_name, prop in page_meta["properties"].items():
                front_matter += f"{prop_name}: {prop}\n"
            front_matter += "\n"
        front_matter += f"# {page_meta['title']}"

        return front_matter

    @staticmethod
    def _add_delimiter_to_table(table_row_blocks: list[dict]) -> list[dict]:
        num_columns = len(table_row_blocks[0]["table_row"]["cells"])
        delimiter_row = {
            "has_children": False,
            "type": "table_row",
            "table_row": {
                "cells": [
                    [
                        {
                            "type": "text",
                            "text": {"content": "---", "link": None},
                            "annotations": {
                                "bold": False,
                                "italic": False,
                                "strikethrough": False,
                                "underline": False,
                                "code": False,
                                "color": "default",
                            },
                            "plain_text": "---",
                            "href": None,
                        }
                    ]
                    for _ in range(num_columns)
                ]
            },
        }

        table_row_blocks.insert(1, delimiter_row)
        return table_row_blocks

    @staticmethod
    def _normalize_id(notion_id: str) -> str:
        # Add dashes to notion id if missing
        if len(notion_id) == 32:
            return (
                notion_id[:8]
                + "-"
                + notion_id[8:12]
                + "-"
                + notion_id[12:16]
                + "-"
                + notion_id[16:20]
                + "-"
                + notion_id[20:]
            )

        elif (
            len(notion_id) != 36
            and notion_id[8] != "-"
            and notion_id[13] != "-"
            and notion_id[18] != "-"
            and notion_id[23] != "-"
        ):
            logger.warning("Notion ID is not in the expected format. ID: %s", notion_id)

        return notion_id
