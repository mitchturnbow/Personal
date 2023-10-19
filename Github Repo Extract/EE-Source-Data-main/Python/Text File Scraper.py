import PySimpleGUI as sg
import os
from pathlib import Path

# def popup_text (filename, text):
#     pop_layout = [
#         [sg.Text(text, key='POPTEXT', size= (80,25))],
#         [sg.Button("OK")]
#     ]
#     pop_win = sg.Window('Scraper Results', pop_layout, modal=True, finalize=True)
#
#     while True:
#         event, values = pop_win.read()
#         if event == "OK" or event == sg.WINDOW_CLOSED:
#             pop_win.close()
#             break
def single_file_win():
    single_layout = [
        [
            [sg.Text('Select File:'), sg.Input(key='S_INPUT_1'), sg.FileBrowse(file_types=(("TXT Files", "*.txt"), ("ALL Files", "*.*")))],  # File Selection
            [sg.Text('Output Directory:'), sg.Input(key='S_INPUT_3'), sg.FolderBrowse(target='S_INPUT_3')], # Set Output Directory
            [sg.Text('Output Filename:'), sg.Input(key='S_INPUT_4')], # Set Output Filename
            [sg.Text('Enter Keyword (Case Sensitive):'), sg.Input(key='S_INPUT_2')], # Set Keyword to be used in scraper
            [sg.Button("Run"), sg.Button("Cancel", button_color=('#ffffff','grey'))],
            # [sg.Text('Output:')], [sg.Output(size=(50,10), key='S_OUTPUT_1')]
        ]
    ]
    single_window = sg.Window('Single File Scraper', single_layout, icon=r'C:\Users\mturnbow\PycharmProjects\Text File Scraper\icon.ico')

    while True:
        event, values = single_window.read()
        if event == 'Cancel' or event == sg.WINDOW_CLOSED:
            single_window.close()
            break
        elif event == 'Run':
            single_file = values['S_INPUT_1']
            single_keyword = values['S_INPUT_2']
            output_dir = values['S_INPUT_3']
            output_file = values['S_INPUT_4']
            if single_file == '':
                sg.Print('Input File Missing.')
            elif single_keyword == '':
                sg.Print('Keyword is Missing.')
            elif output_dir == '' :
                sg.Print('Output Directory Missing.')
            elif output_file == '':
                sg.Print('Output Filename Missing.')
            elif "." not in output_file:
                sg.Print('Output Filename must include file extension.')
            else:
                sg.Print('Scraper Running...\n')
                if Path(single_file).is_file():
                    try:
                        single_matches = 0
                        single_line_num = 0
                        with open(output_dir + "/" + output_file, 'w') as output:
                            sg.Print('Output file created: ' + output_dir + "/" + output_file + '\n')
                            with open(single_file, 'rt', encoding='utf-8') as single_file:
                                for line in single_file:
                                    if single_keyword in line:
                                        single_line_num += 1
                                        single_matches += 1
                                        output.write(line)
                                    else:
                                        single_line_num += 1
                                single_results = ('Scrape complete' + '\n'
                                                                      '\n'
                                                                      'Lines Scraped: ' + str(single_line_num) + '\n'
                                                                      'Matches Found: ' + str(single_matches) + '\n''\n')
                            single_file.close()
                    except Exception as e:
                        print("Error: ", e)
                sg.Print(single_results)
                output.close()
def multi_file_win():
    multi_layout = [
        [
            [sg.Text('Input Directory:'), sg.Input(key='M_INPUT_1'), sg.FolderBrowse(target='M_INPUT_1')],  # Select Input Folder
            [sg.Text('Output Directory:'), sg.Input(key='M_INPUT_3'), sg.FolderBrowse(target='M_INPUT_3')],  # Select Output Folder
            [sg.Text('Output Filename:'), sg.Input(key='M_INPUT_4')],  # Set output filename
            [sg.Text('Enter Keyword (Case Sensitive):'), sg.Input(key='M_INPUT_2')], # Set Keyword to be used in scraper
            [sg.Button("Run"), sg.Button("Cancel", button_color=('#ffffff','grey'))],
            # sg.Text("", size=(0, 1), key='M_OUTPUT_1')
        ]
    ]
    multi_win = sg.Window('Multi File Scraper', multi_layout, icon=r'C:\Users\mturnbow\PycharmProjects\Text File Scraper\icon.ico')

    while True:
        event, values = multi_win.read()
        if event == 'Cancel' or event == sg.WINDOW_CLOSED:
            multi_win.close()
            break
        elif event == 'Run':
            multi_input_src_dir = values['M_INPUT_1']
            multi_keyword = values['M_INPUT_2']
            multi_out_dir = values['M_INPUT_3']
            multi_out_file = values['M_INPUT_4']
            if multi_input_src_dir == '':
                sg.Print('Input Directory Required.')
            elif multi_out_dir == '':
                sg.Print('Output Directory Missing.')
            elif multi_out_file == '':
                sg.Print('Output Filename Missing.')
            elif '.' not in multi_out_file:
                sg.Print('Output Filename must include file extension')
            elif multi_keyword == '':
                sg.Print('Keyword is Missing.')
            else:
                multi_file_list = os.listdir(multi_input_src_dir)
                sg.Print('Scraper Running...\n')
                sg.Print('Output File Created: ' + str(multi_out_dir) + "/" + str(multi_out_file) + '\n')
                if Path(multi_input_src_dir).is_dir():
                    try:
                        multi_match_tot = 0     # Track total number of rows scraped for all files
                        multi_line_tot = 0      # Track total matched found across all files
                        multi_file_count = 0    # Track file count
                        with open(multi_out_dir + "/" + multi_out_file, "w") as multi_out_file:
                            for multi_file in multi_file_list:
                                # sg.Print(multi_file)
                                multi_line_ind = 0      # Track number of rows scraped for each individual file
                                multi_match_ind = 0     # Track matches for each individual file
                                with open(multi_input_src_dir + "/" + multi_file, 'rt', encoding='utf-8') as multi_scrape:
                                    multi_out_file.write(multi_file + '\n')
                                    for line in multi_scrape:
                                        if multi_keyword in line:
                                            multi_line_tot += 1
                                            multi_match_tot += 1
                                            multi_line_ind += 1
                                            multi_match_ind += 1
                                            multi_out_file.write(line)
                                        else:
                                            multi_line_tot += 1
                                            multi_line_ind += 1
                                    multi_scrape.close()
                                    multi_file_count += 1
                                    sg.Print(multi_file + " Results:")
                                    sg.Print("Lines Scraped: " + str(multi_line_ind) + '\nMatches Found: ' + str(multi_match_ind) + '\n')
                            multi_results = ('\nComplete \nFiles Scraped: ' + str(multi_file_count) + '\nTotal Lines Scraped: ' + str(multi_line_tot) + '\nTotal Matches Found: ' + str(multi_match_tot))
                            sg.Print(multi_results + '\n')
                    except Exception as e:
                        print("Error: ", e)



sg.LOOK_AND_FEEL_TABLE['GainwellTheme'] = {'BACKGROUND': '#ffffff',
                                            'TEXT': '#2b3a44',
                                            'INPUT': '#D3D3D3',
                                            'TEXT_INPUT': '#2b3a44',
                                            'SCROLL': '#99CC99',
                                            'BUTTON': ('#2B3A44', '#00EEAF'),
                                            'PROGRESS': ('#D1826B', '#CC8019'),
                                            'BORDER': 1, 'SLIDER_DEPTH': 0,
                                            'PROGRESS_DEPTH': 0, }
sg.theme("GainwellTheme")
sg.set_options(font=("Arial", 10))

main_layout = [
    # [sg.Image(r'C:\Users\mturnbow\PycharmProjects\Text File Scraper\gainwell-logo.png',size=(10,10))],
    [sg.Text("Search Single File or Multiple?")],
    [sg.Button("Single"), sg.Button("Multiple")],
    [sg.Button("Cancel", button_color=('#ffffff','grey'))]
]
main_window = sg.Window("Version Selector", main_layout, size=(300, 100), icon=r'C:\Users\mturnbow\PycharmProjects\Text File Scraper\icon.ico', element_justification='c')

while True:
    event, values = main_window.read()
    if event == 'Cancel' or event == sg.WINDOW_CLOSED:
        break
    elif event == 'Single':
        single_file_win()
    elif event == 'Multiple':
        multi_file_win()

main_window.close()
