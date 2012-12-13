import locale
try:
    locale.setlocale(locale.LC_ALL, '')
except locale.Error:
    locale.setlocale(locale.LC_ALL, 'C')
#print locale.LC_ALL

import os

import gettext

if os.path.isdir('po'):
    # if there is a local directory called 'po' use it so we can test
    # without installing
    #t = gettext.translation('smolt', 'po', fallback = True)
    t = gettext.translation('pylvm', '/usr/share/locale/', fallback = True)
else:
    t = gettext.translation('pylvm', '/usr/share/locale/', fallback = True)

_ = t.gettext
