-- Sites imported from other clients never carry passwords. The flag marks
-- them until the user has reviewed and saved them once in the site form.
ALTER TABLE sites ADD COLUMN needs_credentials INTEGER NOT NULL DEFAULT 0;
