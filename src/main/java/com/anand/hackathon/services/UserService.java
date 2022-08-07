package com.anand.hackathon.services;

import com.anand.hackathon.model.Role;
import com.anand.hackathon.model.User;
import com.anand.hackathon.repository.RoleRepository;
import com.anand.hackathon.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserService {

	@Autowired
	private UserRepository userRepo;
	
	@Autowired
	RoleRepository roleRepo;
	
	@Autowired PasswordEncoder passwordEncoder;
	
	public void registerDefaultUser(User user) {
		Role roleUser = roleRepo.findByName("User");
		user.addRole(roleUser);
		encodePassword(user);
		userRepo.save(user);
	}
	
	public List<User> listAll() {
		return (List<User>) userRepo.findAll();
	}

	public User get(Long id) {
		return userRepo.findById(id).get();
	}
	
	public List<Role> listRoles() {
		return roleRepo.findAll();
	}
	
	public void save(User user) {
		encodePassword(user);		
		userRepo.save(user);
	}
	
	private void encodePassword(User user) {
		String encodedPassword = passwordEncoder.encode(user.getPassword());
		user.setPassword(encodedPassword);

	}
}
