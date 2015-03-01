from colorama import Fore, Back, Style
import string

class Conversation:

    def __init__(self):
        self.Fore = Fore
        self.Back = Back
        self.Style = Style
        pass

    def tell(self, output):
        self.write_terminal(output, Fore.GREEN)

    def ask(self, output):
        self.write_terminal('', Fore.MAGENTA)
        self.write_terminal(output, Fore.MAGENTA)
        self.write_terminal('', Fore.MAGENTA)

    def warn(self, output):
        self.write_terminal('', Fore.RED)
        self.write_terminal(output, Fore.RED)
        self.write_terminal('', Fore.RED)

    @staticmethod
    def write_terminal(lines, promptcolor):
        # Wrap single line in a list
        if isinstance(lines, basestring):
            lines = [lines]

        for line in lines:
            print(promptcolor + '*** ' + Fore.RESET + line)
        pass

    def prompt(self, question):
        self.ask(question)
        answer = raw_input('Please choose\n> ')
        return answer

    def prompt_bool(self, question):
        self.ask([question, 'Yes/No'])

        while True:
            answer = raw_input('Please choose\n> ')
            if 'y' in answer.lower():
                return True
            if 'n' in answer.lower():
                return False

            self.tell(answer + ' is not a valid option, please answer Yes or No')

    def prompt_options(self, optionlist):
        """
        Prompt the user for a selection

        Args:
            options (list): A list of of options

            Each element is a list
                option to prompt for
                option identifier
                (optional) alternative highlight character
        """

        optionpicker = {}
        for elem in optionlist:
            selection_string = elem[0][0]

            # If there's an explicit selection string, don't use the first character
            if len(elem) > 2:
                selection_string = elem[2]

            prepared_option_string = string.replace(elem[0], selection_string, '(' + Style.BRIGHT + selection_string + Style.NORMAL + ')', 1)
            if prepared_option_string == elem[0]:
                raise Exception("Selection string %s not contained in option %s" % (selection_string, elem[0]))

            optionpicker[selection_string] = [prepared_option_string, elem[1]]

        for value in optionpicker.values():
            self.tell(value[0])

        while True:
            picked_option = raw_input('Please choose\n> ')
            if picked_option not in optionpicker:
                self.tell(picked_option + ' is not a valid option, try again')
                continue
            break

        return optionpicker[picked_option][1]
