// ---
// ---

vis_config= 
    
    {"frames": {
        
       "node_table": {
           "insert":       "yes",             
           "class":        "easyui-window",
           "title":        "Node Table",
           "data-options": "inline: false",
           "maximizable":  "false",
           "closable":     "false",
           "src":          "html/table_nodes.html",
           "style":        "left:40px; top:190px; width:285px; height:755px; max-width:285px; max-height:755px;"
        },
        
       "link_table": {
           "insert":       "yes",
           "class":        "easyui-window",
           "title":        "Link Table",
           "data-options": "inline: false",
           "maximizable":  "false",
           "closable":     "false",
           "src":          "html/table_links.html",
           "style":        "left:80px; top:230px; width:520px; height:760px; max-width:520px; max-height: 760px;"
       },
        
       "fd_graph" : {
           "insert":       "yes",
           "class":        "easyui-window",
           "title":        "FD Graph",
           "data-options": "inline: false, collapsed: true",
           "maximizable":  "false",
           "closable":     "false",
           "src":          "html/graph.html",
           "style":        "left:630px; top:190px; width:1000px; height:1000px;"
       },
        
        "tree": {
            "insert":       "yes",
            "class":        "easyui-window",
            "title":        "Tree",
            "data-options": "inline: false, collapsed: true",
            "maximizable":  "false",
            "closable":     "false",
            "src":          "html/tree.html",
            "style":        "left:670px; top:230px; width:970px; height:740px;"
        },
            
        "fd_tree": {
            "insert":       "yes",
            "class":        "easyui-window",
            "title":        "FD Tree",
            "data-options": "inline: false, collapsed: true",
            "maximizable":  "false",
            "closable":     "false",
            "src":          "html/fd_tree.html",
            "style":        "left:710px; top:270px; width:940px; height:760px;"
        }
      },
     
     "buttons": {
         
         "node_table": [{"text": "Show Node Table",
                        "onclick": "$('#node_table').window('open')"},
                        {"text": "Hide Node Table",
                        "onclick": "$('#node_table').window('close')"}
                       ],
        
         "link_table": [{"text": "Show Link Table",
                        "onclick": "$('#link_table').window('open')"},
                        {"text": "Hide Link Table",
                        "onclick": "$('#link_table').window('close')"}
                       ],
        
         "fd_graph" : [{"text": "Show FD Graph Table",
                        "onclick": "$('#fd_graph').window('open')"},
                        {"text": "Hide FD Graph",
                        "onclick": "$('#fd_graph').window('close')"}
                      ],
        
         "tree": [{"text": "Show Tree",
                   "onclick": "$('#tree').window('open')"},
                  {"text": "Hide Tree",
                   "onclick": "$('#tree').window('close')"}
                 ],
            
         "fd_tree": [{"text": "Show FD Tree",
                      "onclick": "$('#fd_tree').window('open')"},
                     {"text": "Hide FD Tree",
                      "onclick": "$('#fd_tree').window('close')"}
                    ]
     }
     
    }

// ---

console.log("*** " 
            + document.getElementsByTagName('script')[0].baseURI 
            + " loaded: vis_config.js" 
            +  " ***"
           );

// ---
// ---