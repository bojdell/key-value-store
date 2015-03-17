#!/bin/bash

# PROJECT_DIR="/Users/bodeckerdellamaria/Documents/Comp\ Sci/cs425/key-value-store"

# echo -e "#!/bin/bash\ncd $PROJECT_DIR\npython ./server-client.py ./conf.txt A" > serverA.sh; chmod +x serverA.sh;
# echo -e "#!/bin/bash\ncd $PROJECT_DIR\npython ./server-client.py ./conf.txt B" > serverB.sh; chmod +x serverB.sh;
# echo -e "#!/bin/bash\ncd $PROJECT_DIR\npython ./server-client.py ./conf.txt C" > serverC.sh; chmod +x serverC.sh;
# echo -e "#!/bin/bash\ncd $PROJECT_DIR\npython ./server-client.py ./conf.txt D" > serverD.sh; chmod +x serverD.sh;

# open -a Terminal ./serverA.sh
# open -a Terminal ./serverB.sh
# open -a Terminal ./serverC.sh
# open -a Terminal ./serverD.sh

. newtab.sh

newtab -G python ./server-client.py ./conf.txt A
newtab -G python ./server-client.py ./conf.txt B
newtab -G python ./server-client.py ./conf.txt C
newtab -G python ./server-client.py ./conf.txt D
newtab -G python ./central-server.py ./conf.txt CENTRAL

echo "Done"