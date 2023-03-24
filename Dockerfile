FROM python:3.10

# Install dependencies
RUN pip install --upgrade pip
ADD requirements.txt /requirements.txt
RUN pip install -r requirements.txt

# Install Google Chrome Stable and fonts
# Note: this installs the necessary libs to make the browser work with Puppeteer.
RUN apt-get update && apt-get install gnupg wget -y && \
  wget --quiet --output-document=- https://dl-ssl.google.com/linux/linux_signing_key.pub | gpg --dearmor > /etc/apt/trusted.gpg.d/google-archive.gpg && \
  sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list' && \
  apt-get update && \
  apt-get install google-chrome-stable -y --no-install-recommends && \
  rm -rf /var/lib/apt/lists/*

# setup non-root user for chrome
RUN groupadd -r pptruser && useradd -r -g pptruser -G audio,video pptruser \
  && mkdir -p /home/pptruser/Downloads \
  && chown -R pptruser:pptruser /home/pptruser

# set working directory to user home
WORKDIR /home/pptruser

# add source code and run as non-root user
ADD . /home/pptruser/
RUN chown -R pptruser:pptruser /home/pptruser
USER pptruser

# Run the app
CMD ["python3", "scraper"]