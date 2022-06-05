from pandas_schema import Column, Schema
from pandas_schema.validation import MatchesPatternValidation

stream_schema=Schema([
        Column('track_metadata.additional_info.artist_msid', [MatchesPatternValidation(r'^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$')], allow_empty=False),
        Column('track_metadata.additional_info.release_msid', [MatchesPatternValidation(r'^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$')], allow_empty=True),
        Column('listened_at', [MatchesPatternValidation(r'^\d{10}$')], allow_empty=False),
        Column('user_name', [], allow_empty=False),
        Column('track_metadata.artist_name', [], allow_empty=False),
        Column('track_metadata.track_name', [], allow_empty=False),
        Column('track_metadata.release_name', [], allow_empty=False),
        Column('track_metadata.additional_info.tracknumber', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('track_metadata.additional_info.dedup_tag', [MatchesPatternValidation(r'^\d+.0$')], allow_empty=True),
        Column('track_metadata.additional_info.discnumber', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('track_metadata.additional_info.duration_ms', [MatchesPatternValidation(r'^\d+.0$')], allow_empty=True),
        Column('track_metadata.additional_info.track_length', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('track_metadata.additional_info.duration', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('track_metadata.additional_info.track_number', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('track_metadata.additional_info.date', [MatchesPatternValidation(r'\d{4}$')], allow_empty=True),
        Column('track_metadata.additional_info.totaldiscs', [MatchesPatternValidation(r'^\d+$')], allow_empty=True),
        Column('track_metadata.additional_info.totaltracks', [MatchesPatternValidation(r'^\d+$')], allow_empty=True)
])
