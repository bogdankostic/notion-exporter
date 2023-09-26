import os
from urllib.parse import urlparse
from collections import defaultdict

from notion2md.convertor.richtext import richtext_convertor


class BlockConverter:
    """
    Converts a Notion block to a Markdown string.
    """

    def __init__(self):
        self.type_specific_converters = {
            "bookmark": self.bookmark,
            "breadcrumb": self.block_without_content,
            "bulleted_list_item": self.bulleted_list_item,
            "callout": self.callout,
            "child_database": self.child_page,
            "child_page": self.child_page,
            "code": self.code,
            "column_list": self.block_without_content,
            "column": self.block_without_content,
            "divider": self.divider,
            "embed": self.embed,
            "equation": self.equation,
            "file": self.file,
            "heading_1": self.heading_1,
            "heading_2": self.heading_2,
            "heading_3": self.heading_3,
            "image": self.image,
            "link_preview": self.link_preview,
            "numbered_list_item": self.numbered_list_item,
            "paragraph": self.paragraph,
            "pdf": self.pdf,
            "quote": self.quote,
            "synced_block": self.block_without_content,
            "table": self.block_without_content,
            "table_of_contents": self.block_without_content,
            "table_row": self.table_row,
            "to_do": self.to_do,
            "toggle": self.toggle,
            "video": self.video,
        }
        self._numbered_list_counters: dict[str, int] = defaultdict(int)

    def convert_block(self, block: dict, indent: bool = False, indent_level: int = 0) -> str:
        """
        Converts a block to a Markdown string.
        """
        block_type = block["type"]
        if block_type in self.type_specific_converters:
            indentation = "\t" * indent_level if indent else ""
            if block_type == "numbered_list_item":
                return indentation + self.numbered_list_item(block, indent_level)
            else:
                return indentation + self.type_specific_converters[block_type](block)
        else:
            return ""

    def bookmark(self, block: dict) -> str:
        """
        Converts a bookmark block to a Markdown link.
        """
        caption = self._get_block_caption(block)
        url = block["bookmark"]["url"]

        return f"[{url}]({url}){caption}"

    @staticmethod
    def block_without_content(block: dict) -> str:
        """
        This method is needed for blocks not containing any content: breadcrumbs, column lists,
        columns, table of contents.
        """
        return ""

    @staticmethod
    def bulleted_list_item(block: dict) -> str:
        """
        Converts a bulleted_list_item block to a Markdown list item.
        """
        text = richtext_convertor(block["bulleted_list_item"]["rich_text"])
        return f"- {text}"

    @staticmethod
    def callout(block: dict) -> str:
        """
        Converts a callout block to a Markdown.
        """
        text = richtext_convertor(block["callout"]["rich_text"])
        icon = block["callout"]["icon"]["emoji"] if "emoji" in block["callout"]["icon"] else ""
        return f"{icon} {text}"

    @staticmethod
    def child_page(block: dict) -> str:
        """
        Converts a child_page or child_database block to a Markdown link.
        """
        block_type = block["type"]  # child_page or child_database
        title = block[block_type]["title"]
        return f"[{title}](www.notion.so/{block['id']})"

    def code(self, block: dict) -> str:
        """
        Converts a code block to a Markdown code block.
        """
        language = block["code"]["language"]
        text = richtext_convertor(block["code"]["rich_text"])
        caption = self._get_block_caption(block)

        return f"```{language}\n{text}\n```{caption}"

    @staticmethod
    def divider(block: dict) -> str:
        """
        Converts a divider block to a Markdown horizontal rule.
        """
        return "___"

    @staticmethod
    def embed(block: dict) -> str:
        """
        Converts an embed block to a Markdown link.
        """
        url = block["embed"]["url"]
        return f"[{url}]({url})"

    @staticmethod
    def equation(block: dict) -> str:
        """
        Converts an equation block to a Markdown equation.
        """
        return f"$$ {block['equation']['expression']} $$"

    def file(self, block: dict) -> str:
        """
        Converts a file block to a Markdown link.
        """
        file_type = block["file"]["type"]
        file_url = block["file"][file_type]["url"]
        file_name = self._get_file_name_from_url(file_url)
        caption = self._get_block_caption(block)

        return f"[{file_name}]({file_url}){caption}"

    @staticmethod
    def heading_1(block: dict) -> str:
        """
        Converts a heading_1 block to a Markdown heading. We are using heading level 2 as heading level 1 is used for
        the page title.
        """
        text = richtext_convertor(block["heading_1"]["rich_text"])
        return f"## {text}"

    @staticmethod
    def heading_2(block: dict) -> str:
        """
        Converts a heading_2 block to a Markdown heading. We are using heading level 3 as heading level 1 is used for
        the page title.
        """
        text = richtext_convertor(block["heading_2"]["rich_text"])
        return f"### {text}"

    @staticmethod
    def heading_3(block: dict) -> str:
        """
        Converts a heading_3 block to a Markdown heading. We are using heading level 4 as heading level 1 is used for
        the page title.
        """
        text = richtext_convertor(block["heading_3"]["rich_text"])
        return f"#### {text}"

    def image(self, block: dict) -> str:
        """
        Converts an image block to a Markdown image.
        """
        file_type = block["image"]["type"]
        file_url = block["image"][file_type]["url"]
        file_name = self._get_file_name_from_url(file_url)
        caption = self._get_block_caption(block)

        return f"![{file_name}]({file_url}){caption}"

    @staticmethod
    def link_preview(block: dict) -> str:
        """
        Converts a link_preview block to a Markdown link.
        """
        url = block["link_preview"]["url"]

        return f"[{url}]({url})"

    def numbered_list_item(self, block: dict, indent_level: int) -> str:
        """
        Converts a numbered_list_item block to a Markdown list item.
        """
        text = richtext_convertor(block["numbered_list_item"]["rich_text"])

        parent_id = block["parent"][block["parent"]["type"]]
        self._numbered_list_counters[parent_id] += 1
        number = self._numbered_list_counters[parent_id]

        return f"{number}. {text}"

    @staticmethod
    def paragraph(block: dict) -> str:
        """
        Converts a paragraph block to a Markdown paragraph.
        """
        text = richtext_convertor(block["paragraph"]["rich_text"])
        return text

    def pdf(self, block: dict) -> str:
        """
        Converts a pdf block to a Markdown link.
        """
        file_type = block["pdf"]["type"]
        file_url = block["pdf"][file_type]["url"]
        file_name = self._get_file_name_from_url(file_url)
        caption = self._get_block_caption(block)

        return f"[{file_name}]({file_url}){caption}"

    @staticmethod
    def quote(block: dict) -> str:
        """
        Converts a quote block to a Markdown quote.
        """
        text = richtext_convertor(block["quote"]["rich_text"])

        return f"> {text}"

    @staticmethod
    def table_row(block: dict) -> str:
        """
        Converts a table_row block to a Markdown table row.
        """
        cells = [richtext_convertor(cell) for cell in block["table_row"]["cells"]]
        row = "|" + "|".join(cells) + "|"
        return row

    @staticmethod
    def to_do(block: dict) -> str:
        """
        Converts a to_do block to a Markdown task list item.
        """
        text = richtext_convertor(block["to_do"]["rich_text"])
        checked = block["to_do"]["checked"]

        return f"- {'[x]' if checked else '[ ]'} {text}"

    @staticmethod
    def toggle(block: dict) -> str:
        """
        Converts a toggle block to a Markdown list item
        (Markdown doesn't have native support for toggle blocks).
        """
        text = richtext_convertor(block["toggle"]["rich_text"])

        return f"- {text}"

    @staticmethod
    def video(block: dict) -> str:
        """
        Converts a video block to a Markdown link.
        """
        caption = richtext_convertor(block["video"]["caption"])
        file_type = block["video"]["type"]
        file_url = block["video"][file_type]["url"]
        file_name = os.path.basename(urlparse(file_url).path)

        return f"[{file_name}]({file_url}){caption}"

    @staticmethod
    def _get_file_name_from_url(url: str) -> str:
        """
        Extracts the file name from a URL.
        """
        return os.path.basename(urlparse(url).path)

    @staticmethod
    def _get_block_caption(block: dict) -> str:
        """
        Extracts the caption from a block.
        """
        block_type = block["type"]
        if block[block_type]["caption"]:
            return f"\n\n{richtext_convertor(block[block_type]['caption'])}"
        else:
            return ""
