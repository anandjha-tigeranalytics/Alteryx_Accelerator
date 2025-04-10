- (optional, but useful for quick runs):
.PHONY: test coverage clean

test:
	pytest

coverage:
	coverage run -m pytest
	coverage html
	open coverage_html_report/index.html

clean:
	rm -rf __pycache__ .pytest_cache .coverage coverage_html_report

