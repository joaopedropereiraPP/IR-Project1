from typing import List


class Posting:
    doc_id: int

    def __init__(self, doc_id: int) -> None:
        self.doc_id = doc_id

    @classmethod
    def from_string(cls, posting_str: str) -> 'Posting':
        return cls(int(posting_str))

    def to_string(self) -> str:
        return str(self.doc_id)


class PostingPositional(Posting):
    positions: List[int]

    def __init__(self, doc_id: int, positions: List[int]) -> None:
        super().__init__(doc_id)
        self.positions = positions

    @classmethod
    def from_string(cls, posting_str: str) -> 'PostingPositional':
        doc_id_str, positions_str = posting_str.split(':')
        positions = list(map(int, positions_str.split(',')))
        return cls(int(doc_id_str), positions)

    def to_string(self) -> str:
        positions_str = (','.join([str(i) for i in self.positions]))
        return super().to_string() + ':' + positions_str


class PostingWeighted(Posting):
    weight: float

    def __init__(self, doc_id: int, weight: float) -> None:
        super().__init__(doc_id)
        self.weight = weight

    @classmethod
    def from_string(cls, posting_str: str) -> 'PostingWeighted':
        doc_id_str, weight_str = posting_str.split(':')
        return cls(int(doc_id_str), float(weight_str))

    def to_string(self) -> str:
        return super().to_string() + ':' + str(self.weight)


class PostingWeightedPositional(PostingWeighted):
    positions: List[int]

    def __init__(self, doc_id: int, weight: float,
                 positions: List[int]) -> None:
        super().__init__(doc_id, weight)
        self.positions = positions

    @classmethod
    def from_string(cls, posting_str: str) -> 'PostingWeightedPositional':
        doc_id_str, weight_str, positions_str = posting_str.split(':')
        positions = list(map(int, positions_str.split(',')))
        return cls(int(doc_id_str), float(weight_str), positions)

    def to_string(self) -> str:
        positions_str = (','.join([str(i) for i in self.positions]))
        return super().to_string() + ':' + positions_str
