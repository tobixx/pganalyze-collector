# Copyright (c) 2014, pganalyze Team <team@pganalyze.com>
#  All rights reserved.

import logging
import sys
from pgacollector.Conversation import Conversation

logger = logging.getLogger(__name__)


class SampleConversation():

    def __init__(self):
        self.c = Conversation()

    def converse(self):

        lines = [
            'This is a demo for the future pganalyze conversation menu',
            'You can learn more about pganalyze at ' + self.c.Style.BRIGHT + 'http://pganalyze.com' + self.c.Style.NORMAL,
            'to continue you need to answer a simple question',
        ]

        self.c.tell(lines)

        options = [
            ['Choose life', 'LIFE'],
            ['Pick death', 'DEATH', 'death'],
            ['Ich will Kuehe', 'moo'],
        ]

        self.c.ask(["What is your greatest desire?"])
        option = self.c.prompt_options(options)
        self.c.tell("You picked %s" % option)

        should_continue = self.c.prompt_bool("Should we go on?")
        if should_continue:
            self.c.tell('Great, moving on')
        else:
            self.c.warn('Blowing up!')
            sys.exit(42)

        answer = self.c.prompt("What is the answer to life and everything?")
        self.c.tell('You picked ' + answer + ' how daring!')

        self.c.warn("Reached the end, aborting!")
