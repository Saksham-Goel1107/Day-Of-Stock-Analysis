const { execSync } = require('child_process');

try {
  const branchName = execSync('git symbolic-ref --short HEAD', { encoding: 'utf8' }).trim();

  if (branchName === 'main') {
    process.exit(0);
  }

  const validTypes = [
    'feature',
    'fix',
    'chore',
    'refactor',
    'test',
    'hotfix',
    'release',
    'docs',
    'style',
    'perf',
    'ci',
    'build',
    'revert',
  ];
  const typePattern = validTypes.join('|');

  const validRegex = new RegExp(`^(${typePattern})\\/[a-z0-9-]+$`);

  if (!validRegex.test(branchName)) {
    console.error(`\n\x1b[31mError: Invalid branch name "${branchName}".\x1b[0m`);
    console.error(`\x1b[33mStrict branch naming is enforced.\x1b[0m`);
    console.error(
      `Branch names must follow the Google-style pattern: \x1b[32mtype/description-slug\x1b[0m`,
    );
    console.error(`Allowed types: \x1b[36m${validTypes.join(', ')}\x1b[0m`);
    console.error(`Example: \x1b[32mfeature/user-dashbaord\x1b[0m or \x1b[32mfix/login-bug\x1b[0m`);
    console.error(`\nThe 'main' branch is exempt from this rule.\n`);
    process.exit(1);
  }
} catch (error) {
  process.exit(0);
}
