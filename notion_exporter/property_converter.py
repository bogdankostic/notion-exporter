import asyncio
from typing import TYPE_CHECKING

from notion2md.convertor.richtext import richtext_convertor

if TYPE_CHECKING:
    from notion_exporter.exporter import NotionExporter


class PropertyConverter:
    """
    Converts Notion property values to Markdown.
    """

    def __init__(self, notion_exporter: "NotionExporter"):
        self.type_specific_converters = {
            "checkbox": self.checkbox,
            "created_by": self.created_by,
            "created_time": self.created_time,
            "date": self.date,
            "email": self.email,
            "files": self.files,
            "formula": self.formula,
            "last_edited_by": self.last_edited_by,
            "last_edited_time": self.last_edited_time,
            "multi_select": self.multi_select,
            "number": self.number,
            "people": self.people,
            "phone_number": self.phone_number,
            "relation": self.relation,
            "rich_text": self.rich_text,
            "rollup": self.rollup,
            "select": self.select,
            "status": self.status,
            "title": self.title,
            "url": self.url,
        }
        self.notion_exporter = notion_exporter

    def convert_property(self, property_item: dict) -> str:
        """
        Converts a Notion property to a Markdown string.
        """
        property_type = property_item["type"]
        return self.type_specific_converters[property_type](property_item)

    @staticmethod
    def checkbox(property_item: dict) -> str:
        """
        Converts a checkbox property to a Markdown checkbox.
        """
        checked = property_item["checkbox"]
        return f"[{'x' if checked else ' '}]"

    @staticmethod
    def created_by(property_item: dict) -> str:
        """
        Converts a created_by property to a Markdown string.
        """
        return property_item.get("created_by", {}).get("name", "Unknown")

    @staticmethod
    def created_time(property_item: dict) -> str:
        """
        Converts a created_time property to a Markdown string.
        """
        return property_item["created_time"]

    @staticmethod
    def date(property_item: dict) -> str:
        """
        Converts a date property to a Markdown string.
        """
        date = ""
        if property_item["date"]:
            if property_item["date"]["start"]:
                date = property_item["date"]["start"]
            if property_item["date"]["end"]:
                date += f" - {property_item['date']['end']}"
        return date

    @staticmethod
    def email(property_item: dict) -> str:
        """
        Converts an email property to a Markdown string.
        """
        email_address = property_item["email"] if property_item["email"] else ""
        return email_address

    @staticmethod
    def files(property_item: dict) -> str:
        file_links = ""
        if property_item["files"]:
            file_links = ", ".join(
                [f"[{file['name']}]({file[file['type']]['url']})" for file in property_item["files"]]
            )
        return file_links

    def formula(self, property_item: dict) -> str:
        """
        Converts a formula property to a Markdown string.
        """
        formula_type = property_item["formula"]["type"]
        if formula_type == "date":
            return self.date(property_item["formula"])
        return str(property_item["formula"][formula_type])

    @staticmethod
    def last_edited_by(property_item: dict) -> str:
        """
        Converts a last_edited_by property to a Markdown string.
        """
        return property_item["last_edited_by"]["name"]

    @staticmethod
    def last_edited_time(property_item: dict) -> str:
        """
        Converts a last_edited_time property to a Markdown string.
        """
        return property_item["last_edited_time"]

    @staticmethod
    def multi_select(property_item: dict) -> str:
        """
        Converts a multi_select property to a Markdown string.
        """
        return ", ".join([option["name"] for option in property_item["multi_select"]])

    @staticmethod
    def number(property_item: dict) -> str:
        """
        Converts a number property to a Markdown string.
        """
        num = str(property_item["number"]) if property_item["number"] is not None else ""
        return num

    @staticmethod
    def people(property_item: dict) -> str:
        """
        Converts a people property to a Markdown string.
        """
        return ", ".join([person["name"] if "name" in person else "Unknown" for person in property_item["people"]])

    @staticmethod
    def phone_number(property_item: dict) -> str:
        """
        Converts a phone_number property to a Markdown string.
        """
        phone_number = property_item["phone_number"] if property_item["phone_number"] else ""
        return phone_number

    def relation(self, property_item: dict) -> str:
        """
        Converts a relation property to a Markdown string.
        """
        if not property_item["relation"]:
            return ""

        related_page_ids = [relation["id"].replace("-", "") for relation in property_item["relation"]]
        return ", ".join([f"www.notion.so/{page_id}" for page_id in related_page_ids])

    @staticmethod
    def rich_text(property_item: dict) -> str:
        """
        Converts a rich_text property to a Markdown string.
        """
        text = richtext_convertor(property_item["rich_text"])
        return text

    @staticmethod
    def rollup(property_item: dict) -> str:
        """
        Converts a rollup property to a Markdown string.
        """
        rollup_type = property_item["rollup"]["type"]
        return str(property_item["rollup"][rollup_type])

    @staticmethod
    def select(property_item: dict) -> str:
        """
        Converts a select property to a Markdown string.
        """
        text = property_item["select"]["name"] if property_item["select"] else ""
        return text

    @staticmethod
    def status(property_item: dict) -> str:
        """
        Converts a status property to a Markdown string.
        """
        text = property_item["status"]["name"] if property_item["status"] else ""
        return text

    @staticmethod
    def title(property_item: dict) -> str:
        """
        Converts a title property to a Markdown string.
        """
        text = property_item["title"][0]["plain_text"] if property_item["title"] else ""
        return text

    @staticmethod
    def url(property_item: dict) -> str:
        """
        Converts a url property to a Markdown string.
        """
        url = property_item["url"] if property_item["url"] else ""
        return url
