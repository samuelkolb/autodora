# autodora [![Build Status](https://travis-ci.org/samuelkolb/autodora.svg?branch=master)](https://travis-ci.org/samuelkolb/autodora)
autodora is a framework to help you:
1. setup experiments
2. running them for multiple parameters
3. storing the results
4. exploring the results

The aim of this package is to make these steps as easy and integrated as possible.

## Installation

    pip install autodora
    
Experiments can be tracked using observers. Specialized observers may require optional packages to function that are
not included by default (because you might not need them).

### Telegram observer

    pip install autodora[telegram]
    
In order to use the observer you have to set the environment variables `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.