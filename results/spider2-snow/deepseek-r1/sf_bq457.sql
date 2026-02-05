WITH "feature_toggle_libraries" AS (
  SELECT 'Unleash.FeatureToggle.Client' AS "artifact_name", 'unleash-client-dotnet' AS "library_name", 'NuGet' AS "platform", 'C#, Visual Basic' AS "languages"
  UNION ALL SELECT 'unleash.client', 'unleash-client', 'NuGet', 'C#, Visual Basic'
  UNION ALL SELECT 'LaunchDarkly.Client', 'launchdarkly', 'NuGet', 'C#, Visual Basic'
  UNION ALL SELECT 'NFeature', 'NFeature', 'NuGet', 'C#, Visual Basic'
  UNION ALL SELECT 'FeatureToggle', 'FeatureToggle', 'NuGet', 'C#, Visual Basic'
  UNION ALL SELECT 'FeatureSwitcher', 'FeatureSwitcher', 'NuGet', 'C#, Visual Basic'
  UNION ALL SELECT 'Toggler', 'Toggler', 'NuGet', 'C#, Visual Basic'
  UNION ALL SELECT 'github.com/launchdarkly/go-client', 'launchdarkly', 'Go', 'Go'
  UNION ALL SELECT 'github.com/xchapter7x/toggle', 'Toggle', 'Go', 'Go'
  UNION ALL SELECT 'github.com/vsco/dcdr', 'dcdr', 'Go', 'Go'
  UNION ALL SELECT 'github.com/unleash/unleash-client-go', 'unleash-client-go', 'Go', 'Go'
  UNION ALL SELECT 'unleash-client', 'unleash-client-node', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT 'ldclient-js', 'launchdarkly', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT 'ember-feature-flags', 'ember-feature-flags', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT 'feature-toggles', 'feature-toggles', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT '@paralleldrive/react-feature-toggles', 'React Feature Toggles', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT 'ldclient-node', 'launchdarkly', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT 'flipit', 'flipit', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT 'fflip', 'fflip', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT 'bandiera-client', 'Bandiera', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT '@flopflip/react-redux', 'flopflip', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT '@flopflip/react-broadcast', 'flopflip', 'NPM', 'JavaScript, TypeScript'
  UNION ALL SELECT 'com.launchdarkly:launchdarkly-android-client', 'launchdarkly', 'Maven', 'Kotlin, Java'
  UNION ALL SELECT 'cc.soham:toggle', 'toggle', 'Maven', 'Kotlin, Java'
  UNION ALL SELECT 'no.finn.unleash:unleash-client-java', 'unleash-client-java', 'Maven', 'Kotlin, Java'
  UNION ALL SELECT 'com.launchdarkly:launchdarkly-client', 'launchdarkly', 'Maven', 'Kotlin, Java'
  UNION ALL SELECT 'org.togglz:togglz-core', 'Togglz', 'Maven', 'Kotlin, Java'
  UNION ALL SELECT 'org.ff4j:ff4j-core', 'FF4J', 'Maven', 'Kotlin, Java'
  UNION ALL SELECT 'com.tacitknowledge.flip:core', 'Flip', 'Maven', 'Kotlin, Java'
  UNION ALL SELECT 'LaunchDarkly', 'launchdarkly', 'CocoaPods', 'Objective-C, Swift'
  UNION ALL SELECT 'launchdarkly/ios-client', 'launchdarkly', 'Carthage', 'Objective-C, Swift'
  UNION ALL SELECT 'launchdarkly/launchdarkly-php', 'launchdarkly', 'Packagist', 'PHP'
  UNION ALL SELECT 'dzunke/feature-flags-bundle', 'Symfony FeatureFlagsBundle', 'Packagist', 'PHP'
  UNION ALL SELECT 'opensoft/rollout', 'rollout', 'Packagist', 'PHP'
  UNION ALL SELECT 'npg/bandiera-client-php', 'Bandiera', 'Packagist', 'PHP'
  UNION ALL SELECT 'UnleashClient', 'unleash-client-python', 'Pypi', 'Python'
  UNION ALL SELECT 'ldclient-py', 'launchdarkly', 'Pypi', 'Python'
  UNION ALL SELECT 'Flask-FeatureFlags', 'Flask FeatureFlags', 'Pypi', 'Python'
  UNION ALL SELECT 'gutter', 'Gutter', 'Pypi', 'Python'
  UNION ALL SELECT 'feature_ramp', 'Feature Ramp', 'Pypi', 'Python'
  UNION ALL SELECT 'flagon', 'flagon', 'Pypi', 'Python'
  UNION ALL SELECT 'django-waffle', 'Waffle', 'Pypi', 'Python'
  UNION ALL SELECT 'gargoyle', 'Gargoyle', 'Pypi', 'Python'
  UNION ALL SELECT 'gargoyle-yplan', 'Gargoyle', 'Pypi', 'Python'
  UNION ALL SELECT 'unleash', 'unleash-client-ruby', 'Rubygems', 'Ruby'
  UNION ALL SELECT 'ldclient-rb', 'launchdarkly', 'Rubygems', 'Ruby'
  UNION ALL SELECT 'rollout', 'rollout', 'Rubygems', 'Ruby'
  UNION ALL SELECT 'feature_flipper', 'FeatureFlipper', 'Rubygems', 'Ruby'
  UNION ALL SELECT 'flip', 'Flip', 'Rubygems', 'Ruby'
  UNION ALL SELECT 'setler', 'Setler', 'Rubygems', 'Ruby'
  UNION ALL SELECT 'bandiera-client', 'Bandiera', 'Rubygems', 'Ruby'
  UNION ALL SELECT 'feature', 'Feature', 'Rubygems', 'Ruby'
  UNION ALL SELECT 'flipper', 'Flipper', 'Rubygems', 'Ruby'
  UNION ALL SELECT 'com.springernature:bandiera-client-scala_2.12', 'Bandiera', 'Maven', 'Scala'
  UNION ALL SELECT 'com.springernature:bandiera-client-scala_2.11', 'Bandiera', 'Maven', 'Scala'
)
SELECT DISTINCT
  "r"."name_with_owner",
  "r"."host_type",
  "r"."size",
  "r"."language",
  "r"."fork_source_name_with_owner",
  "r"."updated_timestamp",
  "ftl"."artifact_name",
  "ftl"."library_name",
  "ftl"."platform",
  "ftl"."languages"
FROM "LIBRARIES_IO"."LIBRARIES_IO"."REPOSITORY_DEPENDENCIES" AS "rd"
INNER JOIN "feature_toggle_libraries" AS "ftl" ON UPPER("rd"."dependency_project_name") = UPPER("ftl"."artifact_name") AND UPPER("rd"."manifest_platform") = UPPER("ftl"."platform")
INNER JOIN "LIBRARIES_IO"."LIBRARIES_IO"."REPOSITORIES" AS "r" ON "rd"."repository_id" = "r"."id"
UNION
SELECT DISTINCT
  "r"."name_with_owner",
  "r"."host_type",
  "r"."size",
  "r"."language",
  "r"."fork_source_name_with_owner",
  "r"."updated_timestamp",
  "ftl"."artifact_name",
  "ftl"."library_name",
  "ftl"."platform",
  "ftl"."languages"
FROM "LIBRARIES_IO"."LIBRARIES_IO"."PROJECTS" AS "p"
INNER JOIN "LIBRARIES_IO"."LIBRARIES_IO"."DEPENDENCIES" AS "d" ON "p"."id" = "d"."project_id"
INNER JOIN "feature_toggle_libraries" AS "ftl" ON UPPER(TRIM("d"."dependency_name")) = UPPER("ftl"."artifact_name") AND UPPER("d"."dependency_platform") = UPPER("ftl"."platform")
INNER JOIN "LIBRARIES_IO"."LIBRARIES_IO"."REPOSITORIES" AS "r" ON "p"."repository_id" = "r"."id"