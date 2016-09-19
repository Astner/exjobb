var tree_trav= (function(){

    // ---

    function getLeafs(children){   

        var leafs;
        var child;

        //console.log(children);

        if(typeof children === "string"){
            leafs= children;

        } else {

            leafs= [];

            for(var i in children){

                child= children[i];

                if (child.hasOwnProperty("children")){
                    leafs= leafs.concat(getLeafs(child.children));

                } 
                else {
                    leafs.push(child.name);
                }
            }
        }
        return leafs;   
    }

    // ---
    
    function getSubTree(path, tree){

        var name;
        var found;
        var child_index;
        var sub_tree= tree.children;

        path.shift();

        for (var path_index in path){

            name= path[path_index];
            found= false;
            child_index= 0;

            do {

                if(sub_tree[child_index].name === name){

                   if (sub_tree[child_index].hasOwnProperty("children")){
                        sub_tree= sub_tree[child_index].children;

                   } else {
                        sub_tree= sub_tree[child_index].name;
                   }

                   found= true;

                } else {

                   child_index= child_index + 1;
                }

            } while (!found);
        }

        return sub_tree;
    }

    // ---
    
    function getParents(dx){  

        var root_name= "concepts";
        var parent_name= dx.name;
        var parent_node= dx;
        var path= [parent_name];

        if (parent_name !== root_name){

            do {

                parent_node= parent_node.parent;
                parent_name= parent_node.name;

                path= [parent_name].concat(path);

            } while(parent_name !== root_name);

        }

        return path;
    }
    
    
// ---
// ---
    
    return {getLeafs:   getLeafs,
            getSubTree: getSubTree,
            getParents: getParents
           };
    
}());

// ---

console.log("*** " 
            + document.getElementsByTagName('script')[0].baseURI 
            + " loaded: tree_trav.js" 
            +  " ***");

