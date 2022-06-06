Task2 -b)

In my solution, I created a dataset with features for clustering users by their listening behavior. 
The features are:

1.  Relative distribution of listening habits over the times categories:
    'afternoon_listener', 'early_morning_listener', 'early_night_listener', 'evening_listener',
    'late_morning_listener' and 'night_listener'

    Based on the user, I calculated the number of streams per time category, divided by the total number of streams, 
    based on the user. This could identify customers to whom e.g. new wake up or fall asleep music can be introduced.
    This may also determine when advertising is sent to a user.

2.  Relative distribution of listening habits over weekdays:
    'Monday_listener', 'Tuesday_listener', 'Wednesday_listener', 
    'Thursday_listener', 'Friday_listener', 'Saturday_listener', 'Sunday_listener' 

    Based on the user, I calculated the number of streams per weekday, divided by the total number of streams.
    This may also determine when advertising is sent to a user or at what day new releases/songs should be released.

3.  Variation coefficient over distinct listened artists, releases and tracks on weekly basis:
    'variation_coefficient_over_listened_artists_on_weekly_basis',
    'variation_coefficient_over_listened_releases_on_weekly_basis',
    'variation_coefficient_over_listened_tracks_on_weekly_basis'

    The variation coefficient is calculated, based on the weekly listening behavior.
    These values express the extent to which the user listens to different music artists, albums or songs.
    Low values tend to describe users who don't change their behavior much and mostly listen 
    the same artists, albums or songs. High values express a frequent change of artists, albums or songs.

    This may make it possible to identify customers to whom new music can be introduced to.
