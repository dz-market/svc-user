-- +goose Up
CREATE TABLE profiles
(
    id uuid PRIMARY KEY,
    created_at timestamptz NOT NULL
);

-- +goose Down
DROP TABLE profiles;
