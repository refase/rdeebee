use std::{borrow::Borrow, cell::RefCell, cmp, mem, rc::Rc};

use uuid::Uuid;

use crate::storage::Event;

/// A left leaning red-black tree, which is simpler to implement than the red-black tree.
/// But has the same properties.
///     1. Every node is colored black or red.
///     2. Red nodes cannot have red children or red parents.
///     3. Root node is always black and every leaf node (always null) is always black.
///     4. Every simple path from any node to all of its descendent leaf nodes has the same "black-depth".
///         (The same number of black nodes)
/// A new node is inserted the same as in a Binary Search Tree (BST). It is assigned the color red.
/// This is because inserting a red node does not violate the black-depth property.
/// After the insertion, the tree is validated.
/// Left Leaning Red Black Trees have one more property:
///     5. No node will have a left black (or null node) and a right red node.
///         If there is then left rotate the node and swap the colors of the current node and its left child.
// TODO: I have some doubts about the rebalancing implementation (from https://github.com/PacktPublishing/Hands-On-Data-Structures-and-Algorithms-with-Rust/blob/master/Chapter05/src/red_black_tree.rs)
//       But for the moment I am going to ignore that.
//       Maybe one of these will fix it:
//          https://brilliant.org/wiki/red-black-tree/
//          https://codereview.stackexchange.com/questions/190041/red-black-tree-in-rust
//          https://codereview.stackexchange.com/questions/263438/rust-persistent-red-black-tree-implementation

type NonNullNodePtr = Rc<RefCell<Node>>;
type NullableNodePtr = Option<NonNullNodePtr>;

#[derive(Debug, PartialEq, Clone)]
enum Color {
    RED,
    BLACK,
}

enum Direction {
    LEFT,
    RIGHT,
}

#[derive(Debug, Clone)]
struct Node {
    event: Event,
    color: Color,
    parent: NullableNodePtr,
    left: NullableNodePtr,
    right: NullableNodePtr,
}

impl Node {
    fn new(event: Event) -> Self {
        Self {
            event,
            color: Color::RED,
            parent: None,
            left: None,
            right: None,
        }
    }
}

#[derive(Debug)]
struct RBTree {
    root: NullableNodePtr,
    length: u64,
}

impl RBTree {
    pub fn new() -> Self {
        // let node = Rc::new(Some(Node::new(event)));
        Self {
            root: None,
            length: 0,
        }
    }

    pub fn height(&self) -> usize {
        let node = self.root.clone();
        self.height_rec(node)
    }

    fn height_rec(&self, node: NullableNodePtr) -> usize {
        let left_height;
        let right_height;
        if let Some(node) = node {
            let left = node.as_ref().borrow().left.clone();
            let right = node.as_ref().borrow().right.clone();
            left_height = self.height_rec(left);
            right_height = self.height_rec(right);
            if left_height > right_height {
                left_height + 1
            } else {
                right_height + 1
            }
        } else {
            0
        }
    }

    pub fn add(&mut self, event: Event) {
        self.length += 1;
        let node = mem::take(&mut self.root);
        let new_root = self.add_rec(node, event);
        self.root = self.fix_tree(new_root.unwrap());
        // self.root = new_root;
    }

    fn add_rec(&mut self, node: NullableNodePtr, event: Event) -> NullableNodePtr {
        match node {
            Some(node) => {
                if node.as_ref().borrow().event.id() <= event.id() {
                    let lft = node.as_ref().borrow_mut().left.take();
                    let new_node = self.add_rec(lft, event).unwrap();
                    new_node.as_ref().borrow_mut().parent = Some(node.clone());
                    node.as_ref().borrow_mut().left = Some(new_node);
                } else {
                    let right = node.as_ref().borrow_mut().right.take();
                    let new_node = self.add_rec(right, event).unwrap();
                    new_node.as_ref().borrow_mut().parent = Some(node.clone());
                    node.as_ref().borrow_mut().right = Some(new_node);
                }
                Some(node)
            }
            None => Some(Rc::new(RefCell::new(Node::new(event)))),
        }
    }

    fn is_a_valid_red_black_tree(&self) -> bool {
        let result = self.validate(self.root.clone(), Color::RED, 0);
        let red_red = result.0;
        let black_height_min = result.1;
        let black_height_max = result.2;
        red_red == 0 && black_height_min == black_height_max
    }

    fn validate(
        &self,
        node: NullableNodePtr,
        parent_color: Color,
        black_height: usize,
    ) -> (usize, usize, usize) {
        if let Some(n) = node {
            let red_red = if parent_color == Color::RED && n.as_ref().borrow().color == Color::RED {
                1
            } else {
                0
            };
            let black_height = black_height
                + match n.as_ref().borrow().color {
                    Color::BLACK => 1,
                    Color::RED => 0,
                };
            let l = self.validate(
                n.as_ref().borrow().left.clone(),
                n.as_ref().borrow().color.clone(),
                black_height,
            );
            let r = self.validate(
                n.as_ref().borrow().right.clone(),
                n.as_ref().borrow().color.clone(),
                black_height,
            );

            (red_red + l.0 + r.0, cmp::min(l.1, r.1), cmp::max(l.2, r.2))
        } else {
            (0, black_height, black_height)
        }
    }

    fn fix_tree(&mut self, node: NonNullNodePtr) -> NullableNodePtr {
        let mut not_root = node.as_ref().borrow().parent.is_some();
        let root = if not_root {
            let mut parent_is_red = node.as_ref().borrow().color == Color::RED;
            let mut n = node.clone();
            while parent_is_red && not_root {
                if let Some(uncle) = self.uncle(node.clone()) {
                    let which = uncle.0;
                    let uncle = uncle.1;

                    match which {
                        Direction::LEFT => {
                            // uncle is lefty
                            let mut parent = n.as_ref().borrow().parent.as_ref().unwrap().clone();
                            if uncle.is_some()
                                && uncle.as_ref().unwrap().as_ref().borrow().color == Color::RED
                            {
                                let uncle = uncle.unwrap();
                                parent.as_ref().borrow_mut().color = Color::BLACK;
                                uncle.as_ref().borrow_mut().color = Color::BLACK;
                                parent
                                    .as_ref()
                                    .borrow_mut()
                                    .parent
                                    .as_ref()
                                    .unwrap()
                                    .as_ref()
                                    .borrow_mut()
                                    .color = Color::RED;
                                n = parent.as_ref().borrow().parent.as_ref().unwrap().clone();
                            } else {
                                if parent.as_ref().borrow().event.id()
                                    > n.as_ref().borrow().event.id()
                                {
                                    // only if left child
                                    let tmp = n.as_ref().borrow().parent.as_ref().unwrap().clone();
                                    n = tmp;
                                    self.rotate(n.clone(), Direction::RIGHT);
                                    parent = n.as_ref().borrow().parent.as_ref().unwrap().clone();
                                }
                                // all black uncles
                                parent.as_ref().borrow_mut().color = Color::BLACK;
                                parent
                                    .as_ref()
                                    .borrow()
                                    .parent
                                    .as_ref()
                                    .unwrap()
                                    .as_ref()
                                    .borrow_mut()
                                    .color = Color::RED;
                                let grandparent = n
                                    .as_ref()
                                    .borrow()
                                    .parent
                                    .as_ref()
                                    .unwrap()
                                    .as_ref()
                                    .borrow()
                                    .parent
                                    .as_ref()
                                    .unwrap()
                                    .clone();
                                self.rotate(grandparent, Direction::LEFT);
                            }
                        }
                        Direction::RIGHT => {
                            let mut parent = n.as_ref().borrow().parent.as_ref().unwrap().clone();

                            if uncle.is_some()
                                && uncle.as_ref().unwrap().as_ref().borrow().color == Color::RED
                            {
                                let uncle = uncle.unwrap();
                                parent.as_ref().borrow_mut().color = Color::BLACK;
                                uncle.as_ref().borrow_mut().color = Color::BLACK;
                                parent
                                    .as_ref()
                                    .borrow_mut()
                                    .parent
                                    .as_ref()
                                    .unwrap()
                                    .as_ref()
                                    .borrow_mut()
                                    .color = Color::RED;
                                n = parent.as_ref().borrow().parent.as_ref().unwrap().clone();
                            } else {
                                if parent.as_ref().borrow().event.id()
                                    > n.as_ref().borrow().event.id()
                                {
                                    // only if right child
                                    let tmp = n.as_ref().borrow().parent.as_ref().unwrap().clone();
                                    n = tmp;
                                    self.rotate(n.clone(), Direction::LEFT);
                                    parent = n.as_ref().borrow().parent.as_ref().unwrap().clone();
                                }
                                // all black uncles
                                parent.as_ref().borrow_mut().color = Color::BLACK;
                                parent
                                    .as_ref()
                                    .borrow_mut()
                                    .parent
                                    .as_ref()
                                    .unwrap()
                                    .as_ref()
                                    .borrow_mut()
                                    .color = Color::RED;
                                let grandparent = n
                                    .as_ref()
                                    .borrow()
                                    .parent
                                    .as_ref()
                                    .unwrap()
                                    .as_ref()
                                    .borrow()
                                    .parent
                                    .as_ref()
                                    .unwrap()
                                    .clone();
                                self.rotate(grandparent, Direction::RIGHT);
                            }
                        }
                    }
                } else {
                    break;
                }
                not_root = n.as_ref().borrow().parent.is_some();
                if not_root {
                    parent_is_red = n
                        .as_ref()
                        .borrow()
                        .parent
                        .as_ref()
                        .unwrap()
                        .as_ref()
                        .borrow()
                        .color
                        == Color::RED;
                }
            }
            while n.as_ref().borrow().parent.is_some() {
                let t = n.as_ref().borrow().parent.as_ref().unwrap().clone();
                n = t;
            }
            Some(n)
        } else {
            Some(node)
        };
        root.map(|r| {
            r.as_ref().borrow_mut().color = Color::BLACK;
            r
        })
    }

    fn rotate(&self, node: NonNullNodePtr, direction: Direction) {
        match direction {
            Direction::RIGHT => {
                let x = node;
                let y = x.as_ref().borrow().left.clone();
                x.as_ref().borrow_mut().left = match y {
                    Some(ref y) => y.as_ref().borrow().right.clone(),
                    None => None,
                };

                if y.is_some() {
                    y.as_ref().unwrap().as_ref().borrow_mut().parent =
                        x.as_ref().borrow().parent.clone();
                    if y.as_ref().unwrap().as_ref().borrow().right.is_some() {
                        let r = y.as_ref().unwrap().as_ref().borrow().right.clone();
                        r.unwrap().as_ref().borrow_mut().parent = Some(x.clone());
                    }
                }

                if let Some(ref parent) = x.as_ref().borrow().parent {
                    if parent.as_ref().borrow().event.id() <= x.as_ref().borrow().event.id() {
                        parent.as_ref().borrow_mut().left = y.clone();
                    } else {
                        parent.as_ref().borrow_mut().right = y.clone();
                    }
                } else {
                    y.as_ref().unwrap().as_ref().borrow_mut().parent = None;
                }
                y.as_ref().unwrap().as_ref().borrow_mut().right = Some(x.clone());
                x.as_ref().borrow_mut().parent = y.clone();
            }
            Direction::LEFT => {
                let x = node;
                let y = x.as_ref().borrow().right.clone();
                x.as_ref().borrow_mut().right = match y {
                    Some(ref y) => y.as_ref().borrow().left.clone(),
                    None => None,
                };

                if y.is_some() {
                    y.as_ref().unwrap().as_ref().borrow_mut().parent =
                        x.as_ref().borrow().parent.clone();
                    if y.as_ref().unwrap().as_ref().borrow().left.is_some() {
                        let l = y.as_ref().unwrap().as_ref().borrow().left.clone();
                        l.unwrap().as_ref().borrow_mut().parent = Some(x.clone());
                    }
                }

                if let Some(ref parent) = x.as_ref().borrow().parent {
                    if parent.as_ref().borrow().event.id() <= x.as_ref().borrow().event.id() {
                        parent.as_ref().borrow_mut().left = y.clone();
                    } else {
                        parent.as_ref().borrow_mut().right = y.clone();
                    }
                } else {
                    y.as_ref().unwrap().as_ref().borrow_mut().parent = None;
                }
                y.as_ref().unwrap().as_ref().borrow_mut().left = Some(x.clone());
                x.as_ref().borrow_mut().parent = y.clone();
            }
        }
    }

    fn uncle(&self, node: NonNullNodePtr) -> Option<(Direction, NullableNodePtr)> {
        let cur = node.as_ref();

        let uncle;
        let side;
        if let Some(ref parent) = cur.borrow().parent.borrow() {
            if let Some(ref grandparent) = parent.as_ref().borrow().parent.borrow() {
                if grandparent.as_ref().borrow().event.id() > parent.as_ref().borrow().event.id() {
                    uncle = grandparent.as_ref().borrow().left.clone();
                    side = Direction::LEFT;
                } else {
                    side = Direction::RIGHT;
                    uncle = grandparent.as_ref().borrow().right.clone();
                }
                return Some((side, uncle.clone()));
            }
        }
        None
    }

    pub fn get_transaction(&self, transaction: Uuid) -> Option<Event> {
        self.find_r(self.root.clone(), transaction)
    }

    fn find_r(&self, node: NullableNodePtr, transaction: Uuid) -> Option<Event> {
        match node {
            Some(n) => {
                if n.as_ref().borrow().event.id() == transaction {
                    Some(n.as_ref().borrow().event.clone())
                } else if n.as_ref().borrow().event.id() <= transaction {
                    self.find_r(n.as_ref().borrow().left.clone(), transaction)
                } else {
                    self.find_r(n.as_ref().borrow().right.clone(), transaction)
                }
            }
            None => None,
        }
    }

    pub(crate) fn walk(&self) {
        self.walk_in_order(&self.root.clone());
    }

    fn walk_in_order(&self, node: &NullableNodePtr) {
        if let Some(node) = node {
            let n = node.as_ref().borrow();
            self.walk_in_order(&n.left);
            println!("{}", n.event);
            self.walk_in_order(&n.right);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::storage::Action;

    #[test]
    fn add_test() {
        let event1 = Event::new(Action::READ);
        let event2 = Event::new(Action::READ);
        let event3 = Event::new(Action::READ);

        let mut rbt = RBTree::new();
        rbt.add(event1);

        rbt.add(event2);

        rbt.add(event3);
        assert_eq!(3, rbt.length);
    }

    #[test]
    fn find_test() {
        let event1 = Event::new(Action::READ);
        let id1 = event1.id();
        let event2 = Event::new(Action::READ);
        let id2 = event2.id();
        let event3 = Event::new(Action::READ);
        let id3 = event3.id();

        let mut rbt = RBTree::new();
        rbt.add(event1);
        rbt.add(event2);
        rbt.add(event3);

        let transaction1 = rbt.get_transaction(id1);
        let transaction2 = rbt.get_transaction(id2);
        let transaction3 = rbt.get_transaction(id3);

        if let Some(t1) = transaction1 {
            assert_eq!(id1, t1.id());
        }

        if let Some(t2) = transaction2 {
            assert_eq!(id2, t2.id());
        }

        if let Some(t3) = transaction3 {
            assert_eq!(id3, t3.id());
        }
    }

    #[test]
    fn validation_test() {
        let mut events = Vec::new();
        for _ in 0..50 {
            events.push(Event::new(Action::READ));
        }

        let mut rbt = RBTree::new();

        for event in events {
            rbt.add(event);
            println!("RBT height: {}", rbt.height());
        }

        assert!(rbt.is_a_valid_red_black_tree());
    }

    #[test]
    fn walk_test() {
        let mut events = Vec::new();
        for _ in 0..15 {
            events.push(Event::new(Action::READ));
        }

        let mut rbt = RBTree::new();

        for event in events {
            rbt.add(event);
            // println!("RBT height: {}", rbt.height());
        }

        rbt.walk();
        // assert_eq!(3, rbt.length);
    }

    #[test]
    fn walk_crate_test() {
        let mut events = Vec::new();
        for _ in 0..15 {
            events.push(Event::new(Action::READ));
        }

        let mut rbt = rbtree::RBTree::new();

        for event in events {
            rbt.insert(event.id(), event);
        }

        println!("RBTree length: {}", rbt.len());
        println!("RBTree: {:#?}", rbt.print_tree());
    }
}
