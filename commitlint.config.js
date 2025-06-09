module.exports = {
  extends: ["@commitlint/config-conventional"],
  // ignore commit messages from dependabot
  ignores: [
    (message) => message.includes("chore(deps): bump"),
    (message) => message.includes("chore(deps-dev): bump"),
  ],
};
