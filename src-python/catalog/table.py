from dataclasses import dataclass, field
from catalog.column import Column
from catalog.index import Index

@dataclass
class Table:
    tab_id: int
    tab_name: str
    tab_namespace: int  # schema id
    tab_tuples: int  # quantity of records
    tab_pages: int  # number of pages
    tab_page_size: int  # size of page
    tab_columns: list[Column] = field(default_factory=list)
    tab_indexes: list[Index] = field(default_factory=list)

    # GETTERS
    def get_tab_id(self) -> int:
        return self.tab_id

    def get_tab_name(self) -> str:
        return self.tab_name

    def get_tab_namespace(self) -> int:
        return self.tab_namespace

    def get_tab_tuples(self) -> int:
        return self.tab_tuples

    def get_tab_pages(self) -> int:
        return self.tab_pages

    def get_tab_page_size(self) -> int:
        return self.tab_page_size

    def get_tab_columns(self) -> list[Column]:
        return self.tab_columns.copy()

    def get_tab_indexes(self) -> list[Index]:
        return self.tab_indexes.copy()

    # METHODS
    def add_column(self, column: Column) -> None:
        self.tab_columns.append(column)

    def add_index(self, index: Index) -> None:
        self.tab_indexes.append(index)