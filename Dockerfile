FROM python:3.10

# Install dependencies
RUN pip install --upgrade pip
ADD requirements.txt /requirements.txt
RUN pip install -r requirements.txt

ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD true

# Install Google Chrome Stable and fonts
# Note: this installs the necessary libs to make the browser work with Puppeteer.
RUN apt-get update && apt-get install gnupg wget -y && \
  wget --quiet --output-document=- https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /etc/apt/trusted.gpg.d/google-archive.gpg && \
  sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list' && \
  apt-get update && \
  apt-get install google-chrome-stable -y --no-install-recommends && \
  rm -rf /var/lib/apt/lists/*

# RUN apt-get install libxtst6 -y
# RUN apt-get install libatk-bridge2.0-0 -y

# Copy source code
ADD . /
WORKDIR /

# Run the app
CMD ["python3", "scraper"]