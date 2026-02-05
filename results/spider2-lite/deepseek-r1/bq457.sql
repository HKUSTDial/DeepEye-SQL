WITH feature_toggle_libraries AS (
  SELECT 'Unleash.FeatureToggle.Client' AS artifact_name, 'unleash-client-dotnet' AS library_name, 'NuGet' AS platform, 'C#, Visual Basic' AS languages UNION ALL
  SELECT 'unleash.client' AS artifact_name, 'unleash-client' AS library_name, 'NuGet' AS platform, 'C#, Visual Basic' AS languages UNION ALL
  SELECT 'LaunchDarkly.Client' AS artifact_name, 'launchdarkly' AS library_name, 'NuGet' AS platform, 'C#, Visual Basic' AS languages UNION ALL
  SELECT 'NFeature' AS artifact_name, 'NFeature' AS library_name, 'NuGet' AS platform, 'C#, Visual Basic' AS languages UNION ALL
  SELECT 'FeatureToggle' AS artifact_name, 'FeatureToggle' AS library_name, 'NuGet' AS platform, 'C#, Visual Basic' AS languages UNION ALL
  SELECT 'FeatureSwitcher' AS artifact_name, 'FeatureSwitcher' AS library_name, 'NuGet' AS platform, 'C#, Visual Basic' AS languages UNION ALL
  SELECT 'Toggler' AS artifact_name, 'Toggler' AS library_name, 'NuGet' AS platform, 'C#, Visual Basic' AS languages UNION ALL
  SELECT 'github.com/launchdarkly/go-client' AS artifact_name, 'launchdarkly' AS library_name, 'Go' AS platform, 'Go' AS languages UNION ALL
  SELECT 'github.com/xchapter7x/toggle' AS artifact_name, 'Toggle' AS library_name, 'Go' AS platform, 'Go' AS languages UNION ALL
  SELECT 'github.com/vsco/dcdr' AS artifact_name, 'dcdr' AS library_name, 'Go' AS platform, 'Go' AS languages UNION ALL
  SELECT 'github.com/unleash/unleash-client-go' AS artifact_name, 'unleash-client-go' AS library_name, 'Go' AS platform, 'Go' AS languages UNION ALL
  SELECT 'unleash-client' AS artifact_name, 'unleash-client-node' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT 'ldclient-js' AS artifact_name, 'launchdarkly' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT 'ember-feature-flags' AS artifact_name, 'ember-feature-flags' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT 'feature-toggles' AS artifact_name, 'feature-toggles' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT '@paralleldrive/react-feature-toggles' AS artifact_name, 'React Feature Toggles' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT 'ldclient-node' AS artifact_name, 'launchdarkly' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT 'flipit' AS artifact_name, 'flipit' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT 'fflip' AS artifact_name, 'fflip' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT 'bandiera-client' AS artifact_name, 'Bandiera' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT '@flopflip/react-redux' AS artifact_name, 'flopflip' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT '@flopflip/react-broadcast' AS artifact_name, 'flopflip' AS library_name, 'NPM' AS platform, 'JavaScript, TypeScript' AS languages UNION ALL
  SELECT 'com.launchdarkly:launchdarkly-android-client' AS artifact_name, 'launchdarkly' AS library_name, 'Maven' AS platform, 'Kotlin, Java' AS languages UNION ALL
  SELECT 'cc.soham:toggle' AS artifact_name, 'toggle' AS library_name, 'Maven' AS platform, 'Kotlin, Java' AS languages UNION ALL
  SELECT 'no.finn.unleash:unleash-client-java' AS artifact_name, 'unleash-client-java' AS library_name, 'Maven' AS platform, 'Kotlin, Java' AS languages UNION ALL
  SELECT 'com.launchdarkly:launchdarkly-client' AS artifact_name, 'launchdarkly' AS library_name, 'Maven' AS platform, 'Kotlin, Java' AS languages UNION ALL
  SELECT 'org.togglz:togglz-core' AS artifact_name, 'Togglz' AS library_name, 'Maven' AS platform, 'Kotlin, Java' AS languages UNION ALL
  SELECT 'org.ff4j:ff4j-core' AS artifact_name, 'FF4J' AS library_name, 'Maven' AS platform, 'Kotlin, Java' AS languages UNION ALL
  SELECT 'com.tacitknowledge.flip:core' AS artifact_name, 'Flip' AS library_name, 'Maven' AS platform, 'Kotlin, Java' AS languages UNION ALL
  SELECT 'LaunchDarkly' AS artifact_name, 'launchdarkly' AS library_name, 'CocoaPods' AS platform, 'Objective-C, Swift' AS languages UNION ALL
  SELECT 'launchdarkly/ios-client' AS artifact_name, 'launchdarkly' AS library_name, 'Carthage' AS platform, 'Objective-C, Swift' AS languages UNION ALL
  SELECT 'launchdarkly/launchdarkly-php' AS artifact_name, 'launchdarkly' AS library_name, 'Packagist' AS platform, 'PHP' AS languages UNION ALL
  SELECT 'dzunke/feature-flags-bundle' AS artifact_name, 'Symfony FeatureFlagsBundle' AS library_name, 'Packagist' AS platform, 'PHP' AS languages UNION ALL
  SELECT 'opensoft/rollout' AS artifact_name, 'rollout' AS library_name, 'Packagist' AS platform, 'PHP' AS languages UNION ALL
  SELECT 'npg/bandiera-client-php' AS artifact_name, 'Bandiera' AS library_name, 'Packagist' AS platform, 'PHP' AS languages UNION ALL
  SELECT 'UnleashClient' AS artifact_name, 'unleash-client-python' AS library_name, 'Pypi' AS platform, 'Python' AS languages UNION ALL
  SELECT 'ldclient-py' AS artifact_name, 'launchdarkly' AS library_name, 'Pypi' AS platform, 'Python' AS languages UNION ALL
  SELECT 'Flask-FeatureFlags' AS artifact_name, 'Flask FeatureFlags' AS library_name, 'Pypi' AS platform, 'Python' AS languages UNION ALL
  SELECT 'gutter' AS artifact_name, 'Gutter' AS library_name, 'Pypi' AS platform, 'Python' AS languages UNION ALL
  SELECT 'feature_ramp' AS artifact_name, 'Feature Ramp' AS library_name, 'Pypi' AS platform, 'Python' AS languages UNION ALL
  SELECT 'flagon' AS artifact_name, 'flagon' AS library_name, 'Pypi' AS platform, 'Python' AS languages UNION ALL
  SELECT 'django-waffle' AS artifact_name, 'Waffle' AS library_name, 'Pypi' AS platform, 'Python' AS languages UNION ALL
  SELECT 'gargoyle' AS artifact_name, 'Gargoyle' AS library_name, 'Pypi' AS platform, 'Python' AS languages UNION ALL
  SELECT 'gargoyle-yplan' AS artifact_name, 'Gargoyle' AS library_name, 'Pypi' AS platform, 'Python' AS languages UNION ALL
  SELECT 'unleash' AS artifact_name, 'unleash-client-ruby' AS library_name, 'Rubygems' AS platform, 'Ruby' AS languages UNION ALL
  SELECT 'ldclient-rb' AS artifact_name, 'launchdarkly' AS library_name, 'Rubygems' AS platform, 'Ruby' AS languages UNION ALL
  SELECT 'rollout' AS artifact_name, 'rollout' AS library_name, 'Rubygems' AS platform, 'Ruby' AS languages UNION ALL
  SELECT 'feature_flipper' AS artifact_name, 'FeatureFlipper' AS library_name, 'Rubygems' AS platform, 'Ruby' AS languages UNION ALL
  SELECT 'flip' AS artifact_name, 'Flip' AS library_name, 'Rubygems' AS platform, 'Ruby' AS languages UNION ALL
  SELECT 'setler' AS artifact_name, 'Setler' AS library_name, 'Rubygems' AS platform, 'Ruby' AS languages UNION ALL
  SELECT 'bandiera-client' AS artifact_name, 'Bandiera' AS library_name, 'Rubygems' AS platform, 'Ruby' AS languages UNION ALL
  SELECT 'feature' AS artifact_name, 'Feature' AS library_name, 'Rubygems' AS platform, 'Ruby' AS languages UNION ALL
  SELECT 'flipper' AS artifact_name, 'Flipper' AS library_name, 'Rubygems' AS platform, 'Ruby' AS languages UNION ALL
  SELECT 'com.springernature:bandiera-client-scala_2.12' AS artifact_name, 'Bandiera' AS library_name, 'Maven' AS platform, 'Scala' AS languages UNION ALL
  SELECT 'com.springernature:bandiera-client-scala_2.11' AS artifact_name, 'Bandiera' AS library_name, 'Maven' AS platform, 'Scala' AS languages
)
SELECT DISTINCT
  r.`name_with_owner` AS repository_full_name,
  r.`host_type` AS hosting_platform_type,
  r.`size` * 1024 AS size_in_bytes,
  r.`language` AS primary_programming_language,
  r.`fork_source_name_with_owner` AS fork_source_name,
  r.`updated_timestamp` AS last_update_timestamp,
  f.`artifact_name`,
  f.`library_name`,
  f.`languages` AS library_languages
FROM `bigquery-public-data.libraries_io`.`repository_dependencies` AS rd
JOIN feature_toggle_libraries AS f
  ON rd.`dependency_project_name` = f.`artifact_name`
  AND UPPER(rd.`manifest_platform`) = UPPER(f.`platform`)
JOIN `bigquery-public-data.libraries_io`.`repositories` AS r
  ON rd.`repository_id` = r.`id`
UNION DISTINCT
SELECT DISTINCT
  r.`name_with_owner` AS repository_full_name,
  r.`host_type` AS hosting_platform_type,
  r.`size` * 1024 AS size_in_bytes,
  r.`language` AS primary_programming_language,
  r.`fork_source_name_with_owner` AS fork_source_name,
  r.`updated_timestamp` AS last_update_timestamp,
  f.`artifact_name`,
  f.`library_name`,
  f.`languages` AS library_languages
FROM `bigquery-public-data.libraries_io`.`dependencies` AS d
JOIN feature_toggle_libraries AS f
  ON d.`dependency_name` = f.`artifact_name`
  AND UPPER(d.`platform`) = UPPER(f.`platform`)
JOIN `bigquery-public-data.libraries_io`.`projects` AS p
  ON d.`project_name` = p.`name`
  AND UPPER(d.`platform`) = UPPER(p.`platform`)
  AND p.`repository_id` IS NOT NULL
JOIN `bigquery-public-data.libraries_io`.`repositories` AS r
  ON p.`repository_id` = r.`id`