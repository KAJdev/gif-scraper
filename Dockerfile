FROM python:3.10

# Install dependencies
RUN pip install --upgrade pip
ADD requirements.txt /requirements.txt
RUN pip install -r requirements.txt

# Copy source code
ADD . /
WORKDIR /

# Run the app
CMD ["python3", "scraper"]