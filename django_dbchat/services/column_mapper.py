import re
from difflib import get_close_matches

class ColumnMapper:
    def __init__(self, schema_columns):
        """
        schema_columns: list of real column names (e.g., ["Customer_Name", "Sales", ...])
        """
        self.schema_columns = schema_columns
        self.normalized_map = {self._normalize(col): col for col in schema_columns}

    def _normalize(self, s):
        return re.sub(r'[^a-z0-9]', '', s.lower())

    def extract_column_terms(self, user_query):
        """
        Extract possible column-like terms from the user query (simple noun phrase extraction)
        """
        # Simple heuristic: split on by, in, of, for, and, or, =, etc.
        tokens = re.split(r'\bby\b|\bin\b|\bof\b|\bfor\b|\band\b|\bor\b|,|=|\(|\)|\n', user_query, flags=re.IGNORECASE)
        terms = [t.strip() for t in tokens if t.strip()]
        # Remove numbers and very short terms
        terms = [t for t in terms if not t.isdigit() and len(t) > 2]
        return terms

    def map_term(self, term):
        """
        Map a user term to the closest real column name
        """
        norm = self._normalize(term)
        # Exact match
        if norm in self.normalized_map:
            return self.normalized_map[norm]
        # Fuzzy match
        matches = get_close_matches(norm, self.normalized_map.keys(), n=1, cutoff=0.7)
        if matches:
            return self.normalized_map[matches[0]]
        # Try splitting on space/underscore
        for col in self.schema_columns:
            if norm in self._normalize(col):
                return col
        return None

    def map_all_terms(self, user_query):
        terms = self.extract_column_terms(user_query)
        mapping = {}
        for term in terms:
            mapped = self.map_term(term)
            if mapped:
                mapping[term] = mapped
        return mapping

    def rewrite_query(self, user_query, mapping):
        """
        Replace user terms in the query with mapped column names
        """
        rewritten = user_query
        for user_term, real_col in mapping.items():
            # Replace case-insensitively
            rewritten = re.sub(re.escape(user_term), real_col, rewritten, flags=re.IGNORECASE)
        return rewritten

    def mapped_schema(self):
        """
        Return only the mapped column names for schema description
        """
        return self.schema_columns 