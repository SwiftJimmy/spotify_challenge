
from pandas_schema import Column, Schema
from pandas_schema.validation import MatchesPatternValidation


stream_schema=Schema([
        Column('artist_msid', [MatchesPatternValidation(r'^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$')], allow_empty=False),
        Column('release_msid', [MatchesPatternValidation(r'^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$')], allow_empty=True),
        Column('listened_at', [MatchesPatternValidation(r'^\d{10}$')], allow_empty=False),
        Column('user_name', [], allow_empty=False),
        Column('artist_name', [], allow_empty=False),
        Column('track_name', [], allow_empty=False),
        Column('release_name', [], allow_empty=False),
        Column('tracknumber', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('dedup_tag', [MatchesPatternValidation(r'^\d+.0$')], allow_empty=True),
        Column('discnumber', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('duration_ms', [MatchesPatternValidation(r'^\d+.0$')], allow_empty=True),
        Column('track_length', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('duration', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('track_number', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('date', [MatchesPatternValidation(r'\d{4}$')], allow_empty=True),
        Column('totaldiscs', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('totaltracks', [MatchesPatternValidation(r'^\d+$')], allow_empty=True)
])

